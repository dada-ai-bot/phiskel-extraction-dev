"""
Configuration loader for External System Integration.

Loads settings from config/settings.yaml and derives:
- Client ID from app name, stage, and instance
- Topics from stage prefix
"""

import os
from pathlib import Path
import yaml
import uuid


class Config:
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Default to config/settings.yaml relative to project root
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config" / "settings.yaml"
        self.config_path = Path(config_path)
        self._project_root = Path(__file__).parent.parent
        self._generated_instance_id = str(uuid.uuid4())[:8]
        self._load_config()

    def _load_config(self):
        with open(self.config_path, "r") as f:
            self._config = yaml.safe_load(f) or {}
            # safe_load returns None if file is empty

    # AWS IoT Core configuration
    @property
    def endpoint(self) -> str:
        return self._config["aws_iot"]["endpoint"]

    @property
    def port(self) -> int:
        return self._config["aws_iot"]["port"]

    @property
    def region(self) -> str:
        return self._config["aws_iot"]["region"]

    # Certificate paths
    @property
    def root_ca_path(self) -> str:
        if os.getenv("IOT_CA_PATH"):
            return os.getenv("IOT_CA_PATH")
        return str(
            self._project_root / self._config["aws_iot"]["certificates"]["root_ca"]
        )

    @property
    def cert_path(self) -> str:
        if os.getenv("IOT_CERT_PATH"):
            return os.getenv("IOT_CERT_PATH")
        return str(
            self._project_root / self._config["aws_iot"]["certificates"]["certificate"]
        )

    @property
    def private_key_path(self) -> str:
        if os.getenv("IOT_PRIVATE_KEY_PATH"):
            return os.getenv("IOT_PRIVATE_KEY_PATH")
        return str(
            self._project_root / self._config["aws_iot"]["certificates"]["private_key"]
        )

    # Application configuration
    @property
    def stage(self) -> str:
        """SST stage name (e.g., 'adam', 'dev', 'production')"""
        return self._config["app"]["stage"]

    @property
    def app_name(self) -> str:
        """Application name from SST"""
        return self._config["app"]["name"]

    @property
    def instance_id(self) -> str:
        """Instance ID for this external system instance"""
        return self._config["client"]["instance_id"]

    # Device capabilities
    @property
    def device_id(self) -> str:
        """
        Unique device ID for this external device.
        Uses client_id as device identifier.
        """
        return self.client_id

    @property
    def capabilities(self) -> dict:
        """Device capabilities (canChat, canTrain, GPU specs)"""
        caps = self._config.get("device", {}).get("capabilities", {})

        # Auto-detect hardware
        gpu_model = caps.get("gpu_model") or self._detect_gpu_model()
        vram_gb = caps.get("vram_gb") or self._detect_vram_gb()
        ram_gb = caps.get("ram_gb") or self._detect_ram_gb()

        # Determine training capability
        # Requirements: NVIDIA GPU, >12GB VRAM
        has_nvidia = gpu_model and "nvidia" in gpu_model.lower()
        has_enough_vram = vram_gb and vram_gb >= 12

        hardware_capable = bool(has_nvidia and has_enough_vram)

        # Allow user to explicitly disable training via config, otherwise use hardware capability
        if caps.get("can_train") is False:
            can_train = False
        else:
            can_train = hardware_capable

        return {
            "canChat": caps.get("can_chat", True),
            "canTrain": can_train,
            "gpuModel": gpu_model,
            "vramGB": vram_gb,
            "ramGB": ram_gb,
            "maxSupportedParams": self._get_max_supported_params(vram_gb),
            "ollamaModels": self._detect_ollama_models(),
        }

    def _get_max_supported_params(self, vram_gb: int) -> str:
        """
        Determine max supported model parameters based on VRAM.
        Assumes 4-bit quantization (AWQ/GPTQ) for these tiers.
        """
        if not vram_gb:
            return "0b"
        if vram_gb >= 24:
            return "32b"
        if vram_gb >= 16:
            return "14b"
        if vram_gb >= 10:  # Allow 10GB-12GB for 8B models
            return "8b"
        return "1b"

    @property
    def metadata(self) -> dict:
        """Device metadata (hostname, cloud provider, etc.)"""
        import socket

        meta = self._config.get("device", {}).get("metadata", {})

        # Auto-detect hostname
        if meta.get("hostname") is None:
            try:
                meta["hostname"] = socket.gethostname()
            except:
                meta["hostname"] = "unknown"

        result = {
            "hostname": meta.get("hostname"),
            "cloudProvider": meta.get("cloud_provider"),
            "region": meta.get("region"),
        }

        # Filter out None values to avoid ElectroDB validation errors
        return {k: v for k, v in result.items() if v is not None}

    def _detect_ollama_models(self) -> list:
        """Attempt to detect installed Ollama models"""
        try:
            import subprocess
            import re

            # Run 'ollama list'
            result = subprocess.run(
                ["ollama", "list"], capture_output=True, text=True, timeout=5
            )

            if result.returncode != 0:
                return []

            # Output format is typically:
            # NAME                ID              SIZE      MODIFIED
            # llama2:latest       78e26419b446    3.8 GB    4 days ago

            models = []
            lines = result.stdout.strip().split("\n")

            # Skip header line
            if len(lines) > 0 and "NAME" in lines[0]:
                lines = lines[1:]

            for line in lines:
                parts = line.split()
                if len(parts) > 0:
                    models.append(parts[0])

            return models
        except Exception as e:
            # logger.warning(f"Failed to detect Ollama models: {e}")
            return []

    def _detect_gpu_model(self) -> str:
        """Attempt to detect GPU model using nvidia-smi"""
        try:
            import subprocess

            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0:
                return result.stdout.strip().split("\n")[0]
        except:
            pass
        return None

    def _detect_vram_gb(self) -> int:
        """Attempt to detect GPU memory using nvidia-smi"""
        try:
            import subprocess

            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=memory.total",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0:
                # Returns MB, convert to GB
                mb = int(float(result.stdout.strip().split("\n")[0]))
                return round(mb / 1024)
        except:
            pass
        return None

    def _detect_ram_gb(self) -> int:
        """Attempt to detect system RAM"""
        try:
            import psutil

            return round(psutil.virtual_memory().total / (1024**3))
        except:
            pass
        return None

    # Runtime configuration
    @property
    def heartbeat_interval(self) -> int:
        """Heartbeat interval in seconds"""
        return self._config.get("runtime", {}).get("heartbeat_interval_seconds", 30)

    # Derived values
    @property
    def client_id(self) -> str:
        """
        Client ID derived from app name, stage, and device ID.
        Format: {app_name}-{stage}-external-system-{device_id}
        """
        # Check for override (used by upload_adapters to avoid conflicts)
        if hasattr(self, "_upload_client_id") and self._upload_client_id:
            return self._upload_client_id

        # If we have a dynamic device ID, use it.
        if self.device_id:
            return f"{self.app_name}-{self.stage}-external-system-{self.device_id}"

        # Fallback for initial connection/registration (temp ID) using instance_id
        return f"{self.app_name}-{self.stage}-external-system-{self.instance_id}"

    @property
    def device_id(self) -> str:
        """
        Unique device ID.
        1. Checks .device_id file
        2. Falls back to instance_id
        """
        id_file = self._project_root / ".device_id"
        if id_file.exists():
            return id_file.read_text().strip()
        return self.instance_id

    @property
    def instance_id(self) -> str:
        """Instance ID from config (fallback)"""
        return self._config.get("client", {}).get(
            "instance_id", self._generated_instance_id
        )

    @property
    def subscribe_topic(self) -> str:
        """
        Topic to subscribe for incoming messages.
        If device_id is present, subscribe to specific topic.
        Otherwise subscribe to broadcast (legacy) or nothing?
        """
        if self.device_id:
            return f"{self.stage}/external/{self.device_id}/request"
        return f"{self.stage}/external/inbound"

    @property
    def publish_topic(self) -> str:
        """
        Topic to publish responses to users.
        Format: {stage}/external/outbound
        Messages are routed to users via ExternalToUserRule.
        """
        return f"{self.stage}/external/outbound"

    # Logging configuration
    @property
    def log_level(self) -> str:
        return self._config.get("logging", {}).get("level", "INFO")

    @property
    def log_format(self) -> str:
        return self._config.get("logging", {}).get(
            "format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
