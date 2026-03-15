"""
Message Router for Forge Service.

Parses incoming IoT envelope messages and routes to processors by originalTopic.
Publishes responses in the format expected by the ExternalToUserRule.

Inbound format for forge persona:
{
  "authenticatedUserId": "cfbb7bec-c4e6-4676-8d6a-822ca65b0321",
  "originalTopic": "adam/forge/new",
  "timestamp": 1769455876127,
  "payload": {
    "upload_id": "cfbb7bec-c4e6-4676-8d6a-822ca65b0321/55286624-4e8a-4a1e-972e-8684169364ed-deathOnTheNile.txt",
    "model": "meta-llama/Llama-3.1-8B",
    "char_name": "bbno$"
  }
}

Outbound format (to {stage}/external/outbound):
{
    "targetUserId": "abc123",
    "payload": {
        "message_id": "msg-001",
        "original_topic": "forge/new",
        "body": { ... }
    }
}
"""

import os
import json
import logging
from typing import Callable

logger = logging.getLogger(__name__)

# Forge service - hardcoded job type
JOB_TYPE = "forge"
logger.info(f"Running in JOB_TYPE: {JOB_TYPE}")

# Import local processors
from src.processors import type_1_execute, type_2_execute_return, type_3_stream

# Processor registry
PROCESSORS = {
    "test/command": type_1_execute,
    "test/answer": type_2_execute_return,
}

# Load forge executor
STAGE = os.getenv("STAGE", "adam")

try:
    from executors import pipeline_executor

    PROCESSORS["+/forge/new"] = pipeline_executor
    PROCESSORS[f"{STAGE}/forge/new"] = pipeline_executor
    logger.info(
        f"Loaded pipeline_executor (Forge). Topics: +/forge/new, {STAGE}/forge/new"
    )
except ImportError as e:
    logger.error(f"Failed to load pipeline_executor: {e}")
    raise


class MessageRouter:
    """Routes incoming messages to appropriate processors based on originalTopic"""

    def __init__(self, mqtt_client):
        """
        Initialize the message router.

        Args:
            mqtt_client: The MqttClient instance for publishing
        """
        self.mqtt_client = mqtt_client
        # Copy global processors to instance to allow modification
        self.processors = PROCESSORS.copy()

        # Override +/forge/new with our custom processor that handles download
        self.processors["+/forge/new"] = ForgeProcessor(self)

        # Also override the specific stage topic if it exists
        stage_forge_topic = f"{STAGE}/forge/new"
        if stage_forge_topic in self.processors:
            self.processors[stage_forge_topic] = ForgeProcessor(self)

    def _topic_matches(self, pattern: str, topic: str) -> bool:
        """
        Check if a topic matches a pattern (supports simple MQTT wildcards + and #).

        Args:
            pattern: Topic pattern (e.g., "+/forge/new")
            topic: Actual topic (e.g., "adam/forge/new")
        """
        if pattern == topic:
            return True

        if "+" not in pattern and "#" not in pattern:
            return False

        p_parts = pattern.split("/")
        t_parts = topic.split("/")

        if len(p_parts) != len(t_parts) and "#" not in p_parts:
            return False

        for i, p in enumerate(p_parts):
            if p == "#":
                return True
            if i >= len(t_parts):
                return False
            if p != "+" and p != t_parts[i]:
                return False

        return len(p_parts) == len(t_parts)

    # Simple in-memory deduplication cache
    _dedup_cache = {}

    # Pending downloads sync
    _pending_downloads = {}

    def route_message(self, payload: bytes) -> None:
        """
        Parse incoming envelope message and route to appropriate processor.

        Args:
            payload: Raw message payload (JSON bytes) in envelope format
        """
        import hashlib
        import time
        import threading

        try:
            envelope = json.loads(payload.decode("utf-8"))

            # Special handling for Download Responses
            if envelope.get("type") == "download-response":
                request_id = envelope.get("requestId")
                if request_id:
                    if request_id in self._pending_downloads:
                        logger.info(f"Received download response for {request_id}")
                        event, container = self._pending_downloads[request_id]
                        container["response"] = envelope
                        event.set()
                    elif (
                        hasattr(self, "_pending_folder_lists")
                        and request_id in self._pending_folder_lists
                    ):
                        logger.info(f"Received folder list response for {request_id}")
                        event, container = self._pending_folder_lists[request_id]
                        container["response"] = envelope
                        event.set()
                return

            # Extract from envelope (IoT Rule format)
            user_id = envelope.get("authenticatedUserId")
            original_topic = envelope.get("originalTopic")
            timestamp = envelope.get("timestamp")
            inner_payload = envelope.get("payload", {})

            message_id = inner_payload.get("message_id")
            body = inner_payload

            # Deduplication Logic
            body_str = json.dumps(body, sort_keys=True)
            msg_hash = hashlib.md5(body_str.encode("utf-8")).hexdigest()
            dedup_key = (user_id, msg_hash)
            current_time = time.time()

            logger.info(f"[Dedup] Checking message from {user_id}")
            logger.info(f"[Dedup] Hash: {msg_hash}")
            logger.info(f"[Dedup] Body: {body_str}")

            keys_to_remove = [
                k for k, v in self._dedup_cache.items() if current_time - v > 5.0
            ]
            for k in keys_to_remove:
                del self._dedup_cache[k]

            last_seen = self._dedup_cache.get(dedup_key)
            if last_seen and (current_time - last_seen < 1.5):
                logger.warning(
                    f"[Dedup] DUPLICATE DETECTED from {user_id} within 1.5s. Hash: {msg_hash}. Ignoring."
                )
                return

            logger.info(f"[Dedup] Message allowed. Adding to cache.")
            self._dedup_cache[dedup_key] = current_time

            if not user_id:
                logger.error(f"Missing authenticatedUserId in envelope: {envelope}")
                return

            if not original_topic:
                logger.error(f"Missing originalTopic in envelope: {envelope}")
                return

            if not message_id:
                import uuid

                message_id = str(uuid.uuid4())
                logger.warning(f"No message_id provided, generated: {message_id}")

            logger.info(f"Received message from {user_id}")
            logger.info(f"  message_id: {message_id}")
            logger.info(f"  original_topic: {original_topic}, timestamp: {timestamp}")

            processor = self.processors.get(original_topic)

            if processor is None:
                for pattern, proc in self.processors.items():
                    if self._topic_matches(pattern, original_topic):
                        processor = proc
                        break

            if processor is None:
                logger.error(f"No processor found for topic: {original_topic}")
                self._publish_error(
                    user_id,
                    message_id,
                    original_topic,
                    f"No processor found for topic: {original_topic}",
                )
                return

            self._execute_processor(
                processor, user_id, message_id, original_topic, body
            )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message JSON: {e}")
        except Exception as e:
            logger.error(f"Error routing message: {e}", exc_info=True)

    def _execute_processor(
        self, processor, user_id: str, message_id: str, original_topic: str, body: dict
    ) -> None:
        """Execute the processor based on its type in a separate thread"""
        import threading

        def run_processor():
            try:
                if hasattr(processor, "STREAMING") and processor.STREAMING:

                    def stream_callback(response_body: dict):
                        self._publish_response(
                            user_id, message_id, original_topic, response_body
                        )

                    processor.process(user_id, message_id, body, stream_callback)
                else:
                    result = processor.process(user_id, message_id, body)

                    if result is not None:
                        self._publish_response(
                            user_id, message_id, original_topic, result
                        )

            except Exception as e:
                logger.error(
                    f"Processor error for topic {original_topic}: {e}", exc_info=True
                )
                self._publish_error(user_id, message_id, original_topic, str(e))

        thread = threading.Thread(target=run_processor)
        thread.daemon = True
        thread.start()

    def _publish_response(
        self, user_id: str, message_id: str, original_topic: str, response_body: dict
    ) -> None:
        """
        Publish a response message in the format expected by ExternalToUserRule.
        """
        import time

        response = {
            "targetUserId": user_id,
            "payload": {
                "message_id": message_id,
                "original_topic": original_topic,
                "timestamp": int(time.time() * 1000),
                **response_body,
            },
        }
        self.mqtt_client._publish_response(response)

    def _publish_error(
        self, user_id: str, message_id: str, original_topic: str, error: str
    ) -> None:
        """Publish an error response"""
        self._publish_response(
            user_id, message_id, original_topic, {"error": True, "message": error}
        )

    # Pending folder list requests
    _pending_folder_lists = {}

    def download_folder(
        self, folder_key: str, timeout: float = 60.0
    ) -> tuple[bool, str]:
        """
        Request file list for a folder and download all files from S3 using MQTT protocol.
        Returns (success, folder_path)
        """
        import uuid
        import threading
        import requests
        from pathlib import Path

        request_id = str(uuid.uuid4())
        event = threading.Event()
        container = {}

        self._pending_folder_lists[request_id] = (event, container)

        try:
            stage = self.mqtt_client.config.stage
            topic = f"{stage}/external/download-request"
            payload = {
                "requestId": request_id,
                "folderKey": folder_key,
                "replyTo": self.mqtt_client.config.subscribe_topic,
            }

            logger.info(
                f"[Download] Requesting file list for folder {folder_key} (req: {request_id})"
            )
            self.mqtt_client.publish(topic, json.dumps(payload))

            if not event.wait(timeout=timeout):
                logger.error(
                    f"[Download] Timeout waiting for folder list response for {folder_key}"
                )
                return False, ""

            response = container.get("response")
            if not response or not response.get("success"):
                error = response.get("error") if response else "No response"
                logger.error(f"[Download] Failed to get folder list: {error}")
                return False, ""

            files = response.get("files", [])
            if not files:
                logger.warning(f"[Download] No files found in folder {folder_key}")
                return False, ""

            logger.info(f"[Download] Found {len(files)} files in folder")

            # Create local folder structure
            base_dir = Path(__file__).parent.parent

            novels_dir = base_dir / "persona_datasets" / "novels"
            other_dir = base_dir / "persona_datasets" / "novelOtherExtensions"

            novels_dir.mkdir(parents=True, exist_ok=True)
            other_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f"[Download] Downloading .txt to {novels_dir}")
            logger.info(f"[Download] Downloading others to {other_dir}")

            download_results = []
            for file_info in files:
                file_key = file_info.get("key")
                download_url = file_info.get("downloadUrl")
                filename = file_info.get("filename")

                if not download_url or not filename:
                    logger.warning(
                        f"[Download] Skipping file with missing info: {file_info}"
                    )
                    continue

                if filename.lower().endswith(".txt"):
                    target_path = novels_dir / filename
                else:
                    target_path = other_dir / filename

                try:
                    logger.info(f"[Download] Downloading {filename}")
                    with requests.get(download_url, stream=True) as r:
                        r.raise_for_status()
                        with open(target_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    logger.info(
                        f"[Download] Successfully downloaded {filename} to {target_path}"
                    )
                    download_results.append(True)
                except Exception as e:
                    logger.error(f"[Download] Failed to download {filename}: {e}")
                    download_results.append(False)

            if all(download_results):
                logger.info(
                    f"[Download] Successfully downloaded all {len(files)} files"
                )
                return True, str(novels_dir)
            else:
                failed_count = download_results.count(False)
                logger.error(
                    f"[Download] {failed_count} out of {len(files)} files failed to download"
                )
                return False, ""

        except Exception as e:
            logger.error(f"[Download] Exception during folder download: {e}")
            return False, ""
        finally:
            self._pending_folder_lists.pop(request_id, None)

    def process_forge_new(
        self, user_id: str, message_id: str, body: dict, callback=None
    ) -> dict:
        """
        Handle +/forge/new: Download folder then run pipeline.
        """
        logger.info(f"[Forge] Processing forge request for {user_id}")

        logger.info(f"[Forge] Body keys: {list(body.keys())}")

        folder_key = (
            body.get("folder_name") or body.get("folderId") or body.get("folderKey")
        )
        file_key = body.get("fileKey") or body.get("file_key") or body.get("upload_id")

        target_path = None

        if folder_key:
            logger.info(
                f"[Forge] Found folder_name: {folder_key}. Initiating folder download."
            )
            success, path = self.download_folder(folder_key)
            if not success:
                error_response = {
                    "status": "error",
                    "message": "Failed to download folder from S3",
                    "error": True,
                }
                if callback:
                    callback(error_response)
                return error_response
            target_path = path
            body["target_path"] = target_path

        elif file_key:
            logger.info(
                f"[Forge] Found single fileKey: {file_key}. LEGACY MODE - deprecated."
            )
            error_response = {
                "status": "error",
                "message": "Legacy single-file processing not supported. Please use folder upload.",
                "error": True,
            }
            if callback:
                callback(error_response)
            return error_response

        # Check for Model Adapters (On-Demand Download)
        model_name = body.get("model")
        if model_name:
            logger.info(f"[Forge] Checking if model '{model_name}' needs download...")
            updated_model = self._prepare_adapter(model_name)
            if updated_model:
                logger.info(f"[Forge] Model path resolved to: {updated_model}")
                body["model"] = updated_model

        extraction_model = body.get("extractionModel")
        if extraction_model:
            logger.info(
                f"[Forge] Checking if extraction model '{extraction_model}' needs download..."
            )
            updated_extraction_model = self._prepare_adapter(extraction_model)
            if updated_extraction_model:
                logger.info(
                    f"[Forge] Extraction model path resolved to: {updated_extraction_model}"
                )
                body["extractionModel"] = updated_extraction_model

        if not folder_key and not file_key:
            logger.warning(
                "[Forge] No folder_name/folderKey found in payload. Skipping dataset download."
            )

        # Call the pipeline executor
        result = pipeline_executor.process(user_id, message_id, body, callback=callback)

        if callback and result:
            callback(result)

        return result

    def _prepare_adapter(self, adapter_name: str, base_model: str = None) -> str:
        """
        Check if adapter is locally available, download from S3 if missing.

        Args:
            adapter_name: Adapter ID (e.g. "hercule_poirot-Qwen3-8B-Q4-20260212-164332")
            base_model: Base HuggingFace model name (e.g. "Qwen/Qwen3-8B")

        Returns:
            str: Absolute path to adapter if found/downloaded, or None on failure.
        """
        from pathlib import Path

        logger.info(f"[Adapter] Checking availability for adapter: {adapter_name}")

        base_dir = Path(__file__).parent.parent
        adapter_dir = base_dir / "persona_datasets" / "adapters" / adapter_name

        if adapter_dir.exists() and any(adapter_dir.iterdir()):
            logger.info(f"[Adapter] Found local adapter at {adapter_dir}")
            return str(adapter_dir.absolute())

        logger.info(
            f"[Adapter] Adapter {adapter_name} not found locally. Initiating download..."
        )

        if base_model:
            folder_key = f"{base_model}/{adapter_name}"
        else:
            folder_key = adapter_name
            logger.warning(
                f"[Adapter] No base_model provided. Using adapter_name as folder_key: {folder_key}"
            )

        success = self._download_adapter_specific(folder_key, adapter_dir)

        if success:
            return str(adapter_dir.absolute())
        else:
            logger.error(f"[Adapter] Failed to download adapter {adapter_name}.")
            return None

    def _download_adapter_specific(self, folder_key: str, target_dir: object) -> bool:
        """
        Download adapter files from S3 to target_dir.

        Args:
            folder_key: Full S3 folder key (e.g. "Qwen/Qwen3-8B/hercule_poirot-...")
            target_dir: Local Path to save adapter files to
        """
        import uuid
        import threading
        import requests

        logger.info(f"[Adapter] Downloading {folder_key} to {target_dir}")
        target_dir.mkdir(parents=True, exist_ok=True)

        request_id = str(uuid.uuid4())
        event = threading.Event()
        container = {}

        self._pending_folder_lists[request_id] = (event, container)

        try:
            topic = f"{self.mqtt_client.config.stage}/external/download-request"

            payload = {
                "requestId": request_id,
                "folderKey": folder_key,
                "replyTo": self.mqtt_client.config.subscribe_topic,
            }

            logger.info(f"[Adapter] Requesting file list for: {folder_key}")
            self.mqtt_client.publish(topic, json.dumps(payload))

            if not event.wait(timeout=120):
                logger.error(
                    f"[Adapter] Timeout waiting for file list for {folder_key}"
                )
                return False

            response = container.get("response")
            if not response or not response.get("success"):
                logger.error(f"[Adapter] Failed to get file list: {response}")
                return False

            files = response.get("files", [])
            if not files:
                logger.warning(f"[Adapter] No files found for {folder_key}")
                return False

            for file_info in files:
                filename = file_info.get("filename")
                download_url = file_info.get("downloadUrl")

                if not filename or not download_url:
                    continue

                file_path = target_dir / filename

                with requests.get(download_url, stream=True) as r:
                    r.raise_for_status()
                    with open(file_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)

            logger.info(f"[Adapter] Successfully downloaded {folder_key}")
            return True

        except Exception as e:
            logger.error(f"[Adapter] Download failed: {e}")
            return False
        finally:
            self._pending_folder_lists.pop(request_id, None)


class ForgeProcessor:
    """Wrapper to allow MessageRouter method to be used as a processor"""

    STREAMING = True

    def __init__(self, router):
        self.router = router

    def process(self, user_id, message_id, body, callback=None):
        return self.router.process_forge_new(user_id, message_id, body, callback)
