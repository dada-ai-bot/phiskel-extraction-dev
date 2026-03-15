"""
Forge Service - Training Pipeline + MQTT Communication

Entry point for the AWS IoT Core forge system integration.
Supports two modes:
  1. IoT listener mode: Subscribes to MQTT, waits for forge commands
  2. Nosana standalone mode: Processes a payload file and exits
"""

import logging
import signal
import sys
import time
import threading
import os
from pathlib import Path

# Add project root to path to allow imports from src and executors
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import Config
from src.mqtt_client import MqttClient
from src.heartbeat import HeartbeatSender
from awscrt import mqtt
import argparse
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Run Mode Detection
# NOSANA_MODE or FORGE_PAYLOAD = standalone mode (process payload and exit)
# Otherwise = IoT listener mode (wait for MQTT messages)
RUN_MODE_NOSANA = bool(os.environ.get("NOSANA_MODE") or os.environ.get("FORGE_PAYLOAD"))


def send_device_registration(mqtt_client, config):
    """Send device registration message to AWS IoT"""
    import json

    topic = f"{config.stage}/devices/register"

    # Get capabilities (includes auto-detected ollama models)
    capabilities = config.capabilities

    payload = {
        "deviceId": config.device_id,
        "capabilities": capabilities,
        "metadata": config.metadata,
        "ollama_models": capabilities.get("ollamaModels", []),
    }

    logger.info("==" * 30)
    logger.info("DEVICE REGISTRATION")
    logger.info("==" * 30)
    logger.info(f"Device ID:    {config.device_id}")
    logger.info(f"Capabilities:")
    logger.info(f"  Can Chat:   {capabilities['canChat']}")
    logger.info(f"  Can Train:  {capabilities['canTrain']}")
    if capabilities["gpuModel"]:
        logger.info(f"  GPU Model:  {capabilities['gpuModel']}")
    if capabilities["vramGB"]:
        logger.info(f"  VRAM:       {capabilities['vramGB']} GB")
    if capabilities["ramGB"]:
        logger.info(f"  RAM:        {capabilities['ramGB']} GB")

    models = capabilities.get("ollamaModels", [])
    if models:
        logger.info(f"  Ollama Models ({len(models)}):")
        for m in models:
            logger.info(f"    - {m}")
    else:
        logger.info("  Ollama Models: None detected")

    logger.info("==" * 30)

    try:
        mqtt_client.publish(topic, json.dumps(payload))
        logger.info("✓ Device registration sent")
    except Exception as e:
        logger.error(f"Failed to send registration: {e}")
        raise


def perform_initial_setup(config):
    """
    Perform initial device ID negotiation if no ID exists.
    Returns: New Config object with valid Device ID.
    """
    if config.device_id:
        return config

    logger.info("No Device ID found. Starting Dynamic Registration...")

    import uuid
    import json

    # Create temp client for negotiation
    temp_id = str(uuid.uuid4())

    # Force a unique instance ID for negotiation to avoid conflicts with existing workers
    negotiation_instance_id = f"negotiation-{temp_id[:8]}"

    if "client" not in config._config:
        config._config["client"] = {}
    config._config["client"]["instance_id"] = negotiation_instance_id

    logger.info(f"Using temporary identity for negotiation: {config.client_id}")

    client = MqttClient(config)
    client.connect()

    setup_complete = threading.Event()
    new_device_id_container = {}

    def on_assign_response(topic, payload, **kwargs):
        try:
            data = json.loads(payload.decode("utf-8"))
            new_id = data.get("newDeviceId")
            if new_id:
                logger.info(f"Received new Device ID: {new_id}")
                new_device_id_container["id"] = new_id
                setup_complete.set()
        except Exception as e:
            logger.error(f"Error parsing assign response: {e}")

    response_topic = f"{config.stage}/devices/assign_id/response/{temp_id}"

    subscribe_future, _ = client.connection.subscribe(
        topic=response_topic, qos=mqtt.QoS.AT_LEAST_ONCE, callback=on_assign_response
    )
    subscribe_future.result()

    request_topic = f"{config.stage}/devices/assign_id"
    client.publish(request_topic, json.dumps({"tempId": temp_id}))

    logger.info("Waiting for ID assignment...")
    if not setup_complete.wait(timeout=30):
        logger.error("Timed out waiting for ID assignment!")
        client.disconnect()
        sys.exit(1)

    new_id = new_device_id_container["id"]
    id_file = Path(__file__).parent.parent / ".device_id"
    id_file.write_text(new_id)
    logger.info(f"Saved Device ID to {id_file}")

    client.disconnect()

    return Config()


def send_device_deregistration(mqtt_client, config):
    """Send device deregistration message on shutdown"""
    import json

    topic = f"{config.stage}/devices/deregister"
    payload = {"deviceId": config.device_id}

    try:
        mqtt_client.publish(topic, json.dumps(payload))
        logger.info("✓ Device deregistration sent")
    except Exception as e:
        logger.error(f"Failed to send deregistration: {e}")


def run_nosana_pipeline(payload_path: str):
    """
    Run the training pipeline in Nosana mode (standalone, no MQTT).

    Args:
        payload_path: Path to JSON file containing the forge payload
    """
    logger.info("=" * 60)
    logger.info("NOSANA STANDALONE MODE")
    logger.info("=" * 60)

    # Load payload from file
    with open(payload_path, "r") as f:
        message_body = json.load(f)

    logger.info(f"Loaded payload: {json.dumps(message_body, indent=2)}")

    # Initialize SINGLE Persistent MQTT Client and Heartbeat
    nosana_client = None
    heartbeat = None

    try:
        # Create Config and generate robust unique Client ID
        config = Config()
        unique_suffix = f"nosana-{int(time.time())}-{os.urandom(4).hex()}"
        config._upload_client_id = (
            f"{config.app_name}-{config.stage}-external-system-{unique_suffix}"
        )

        logger.info(f"Initializing Persistent MQTT Client: {config.client_id}")

        nosana_client = MqttClient(config)
        nosana_client.connect()

        # Start Heartbeat immediately to keep connection alive
        heartbeat = HeartbeatSender(nosana_client, config)
        heartbeat.start()
        logger.info("✅ Heartbeat started - MQTT connection active")

        # Import and run pipeline executor directly
        from executors.pipeline_executor import process as execute_pipeline

        # Extract authenticated user ID from payload
        user_id = message_body.get("authenticatedUserId", "unknown")
        payload = message_body.get("payload", {})

        logger.info(f"Running pipeline for user: {user_id}")
        logger.info(f"Character: {payload.get('char_name')}")
        logger.info(f"Model: {payload.get('model')}")

        # Check if we need to download files from S3
        folder_key = payload.get("folder_name") or payload.get("folderId")

        if folder_key:
            logger.info(
                f"folder_name detected: {folder_key}. Downloading files from S3..."
            )

            import requests
            from pathlib import Path

            request_id = f"nosana-download-{int(time.time() * 1000)}"
            stage = config.stage

            response_topic = f"{stage}/external/download-response/{request_id}"

            topic = f"{stage}/external/download-request"
            download_payload = {
                "requestId": request_id,
                "folderKey": folder_key,
                "replyTo": response_topic,
            }

            logger.info(f"[Download] Requesting file list (req: {request_id})")

            logger.info(f"[Download] Subscribing to response topic: {response_topic}")
            nosana_client.subscribe_to_topic(response_topic)

            nosana_client.publish(topic, json.dumps(download_payload))

            response = nosana_client.wait_for_message(
                response_topic, request_id, timeout=60
            )

            if not response or not response.get("success"):
                error = response.get("error") if response else "No response"
                logger.error(f"[Download] Failed to get folder list: {error}")
                raise RuntimeError(f"Download failed: {error}")

            files = response.get("files", [])
            if not files:
                logger.warning(f"[Download] No files found in folder {folder_key}")
                raise RuntimeError("No files found to download")

            logger.info(f"[Download] Found {len(files)} files to download")

            # Create local folder structure
            base_dir = Path("/workspace")
            novels_dir = base_dir / "persona_datasets" / "novels"
            other_dir = base_dir / "persona_datasets" / "novelOtherExtensions"

            novels_dir.mkdir(parents=True, exist_ok=True)
            other_dir.mkdir(parents=True, exist_ok=True)

            # Download all files
            download_results = []
            for file_info in files:
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
                    logger.info(f"[Download] ✓ Downloaded {filename}")
                    download_results.append(True)
                except Exception as e:
                    logger.error(f"[Download] Failed to download {filename}: {e}")
                    download_results.append(False)

            if not all(download_results):
                failed_count = download_results.count(False)
                logger.error(
                    f"[Download] {failed_count} out of {len(files)} files failed!"
                )
                raise RuntimeError("Failed to download some files")

            logger.info(f"[Download] Successfully downloaded all {len(files)} files")
        else:
            logger.warning(
                "[Download] No folder_name in payload. Skipping S3 download."
            )

        # Prepare outbound topic for notifications
        outbound_topic = f"{config.stage}/external/outbound"

        def pipeline_callback(response_body: dict):
            """Callback to send pipeline updates (e.g. started) via MQTT"""
            logger.info(
                f"Sending pipeline update to {outbound_topic}: {response_body.get('message', '')}"
            )

            response = {
                "targetUserId": user_id,
                "payload": {
                    "message_id": "nosana-job",
                    "original_topic": f"{config.stage}/forge/new",
                    "timestamp": int(time.time() * 1000),
                    **response_body,
                },
            }
            nosana_client.publish(outbound_topic, json.dumps(response))

        # Execute pipeline and CAPTURE result
        pipeline_result = execute_pipeline(
            user_id, "nosana-job", message_body, callback=pipeline_callback
        )

        if not pipeline_result or pipeline_result.get("status") != "success":
            logger.error(f"Pipeline failed: {pipeline_result.get('message')}")
            sys.exit(1)

        logger.info("Pipeline execution completed successfully")

        # Use the ID returned by the pipeline script (which it used for S3 upload)
        adapter_id = pipeline_result.get("model_name")

        if not adapter_id:
            logger.error("Pipeline did not return a valid model_name (adapter_id)!")
            sys.exit(1)

        logger.info(f"Using pipeline-generated adapter_id: {adapter_id}")

        success_message = {
            "targetUserId": user_id,
            "payload": {
                "message_id": "nosana-job",
                "original_topic": f"{config.stage}/forge/new",
                "timestamp": int(time.time() * 1000),
                "status": "success",
                "message": "Pipeline completed successfully",
                "character_name": payload.get("char_name"),
                "model_name": adapter_id,
                "base_model": payload.get("model"),
                "output": "Pipeline finished (Nosana mode)",
                "done": True,
                "specifications": payload.get("specifications", {}),
            },
        }

        outbound_topic = f"{config.stage}/external/outbound"
        logger.info(f"Sending success notification to {outbound_topic}")
        nosana_client.publish(outbound_topic, json.dumps(success_message))
        logger.info("✓ Success notification sent (model will be created)")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        sys.exit(1)

    finally:
        logger.info("Cleaning up resources...")
        if heartbeat:
            try:
                heartbeat.stop()
                logger.info("Heartbeat stopped")
            except:
                pass

        if nosana_client:
            try:
                time.sleep(1)
                nosana_client.disconnect()
                logger.info("MQTT Disconnected")
            except:
                pass

    logger.info("=" * 60)
    logger.info("NOSANA JOB COMPLETE")
    logger.info("=" * 60)


def main():
    """Main entry point - supports both normal mode and Nosana mode."""
    parser = argparse.ArgumentParser(
        description="Forge Service - Training Pipeline + MQTT"
    )
    parser.add_argument(
        "--nosana", action="store_true", help="Run in Nosana standalone mode"
    )
    parser.add_argument(
        "--payload", type=str, help="Path to payload JSON file (Nosana mode only)"
    )
    args = parser.parse_args()

    # Check for Nosana mode
    if args.nosana:
        if not args.payload:
            logger.error("--payload argument required in Nosana mode")
            sys.exit(1)
        run_nosana_pipeline(args.payload)
        return

    # Normal mode - MQTT device
    logger.info("Starting Forge Service in IoT device mode")

    # Load configuration
    try:
        initial_config = Config()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Perform handshake if needed
    config = perform_initial_setup(initial_config)

    logger.info("=" * 60)
    logger.info("Forge Service - Pipeline + MQTT")
    logger.info("=" * 60)
    logger.info(f"Stage:           {config.stage}")
    logger.info(f"App Name:        {config.app_name}")
    logger.info(f"Device ID:       {config.device_id}")
    logger.info(f"Subscribe Topic: {config.subscribe_topic}")
    logger.info(f"Publish Topic:   {config.publish_topic}")
    logger.info(f"Endpoint:        {config.endpoint}")
    logger.info("=" * 60)

    # Create MQTT client
    client = MqttClient(config)

    # Create heartbeat sender
    heartbeat = HeartbeatSender(client, config)

    # Shutdown flag for main loop
    shutdown_requested = threading.Event()

    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Shutting down...")
        shutdown_requested.set()

        try:
            heartbeat.stop()
            send_device_deregistration(client, config)
            time.sleep(0.5)
            client.disconnect()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

        import os

        os._exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Connect to AWS IoT Core
        client.connect()

        # Send device registration
        time.sleep(2)
        send_device_registration(client, config)

        # Start heartbeat
        heartbeat.start()

        # Subscribe to device-specific request topic
        client.subscribe()

        logger.info("Forge service running. Press Ctrl+C to stop.")
        logger.info("Waiting for messages...")

        while True:
            time.sleep(1)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        try:
            client.disconnect()
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
