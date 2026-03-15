"""
Device Heartbeat Module

Sends periodic heartbeat messages to AWS IoT to maintain device status as 'online'.
Heartbeat includes current load (active chat and training requests).
"""

import logging
import threading
import time
import json

logger = logging.getLogger(__name__)


class HeartbeatSender:
    """Manages periodic heartbeat messages to AWS IoT"""

    def __init__(self, mqtt_client, config):
        """
        Initialize heartbeat sender.

        Args:
            mqtt_client: MqttClient instance
            config: Config instance
        """
        self.mqtt_client = mqtt_client
        self.config = config
        self.interval = config.heartbeat_interval
        self.running = False
        self.thread = None

    def start(self):
        """Start heartbeat thread"""
        if self.running:
            logger.warning("Heartbeat already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.thread.start()
        logger.info(f"Heartbeat started (interval: {self.interval}s)")

    def stop(self):
        """Stop heartbeat thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Heartbeat stopped")

    def _heartbeat_loop(self):
        """Heartbeat loop - sends periodic heartbeat messages"""
        while self.running:
            try:
                self._send_heartbeat()
            except Exception as e:
                logger.error(f"Heartbeat send failed: {e}")

            # Sleep in small increments to allow fast shutdown
            for _ in range(self.interval):
                if not self.running:
                    break
                time.sleep(1)

    def _send_heartbeat(self):
        """Send heartbeat message to AWS IoT"""
        topic = f"{self.config.stage}/devices/heartbeat"

        # Get capabilities again in case models changed (e.g. user pulled new model)
        # Note: calling config.capabilities re-runs detection due to the property getter in config.py
        caps = self.config.capabilities

        # Get current load from message router
        # For now, we'll send 0 for both - the router will track this later
        payload = {
            "deviceId": self.config.device_id,
            "chatRequestsActive": 0,  # TODO: Track from message router
            "trainRequestsActive": 0,  # TODO: Track from message router
            "ollama_models": caps.get("ollamaModels", []),
            "vramGB": caps.get("vramGB"),
            "maxSupportedParams": caps.get("maxSupportedParams"),
        }

        try:
            self.mqtt_client.publish(topic, json.dumps(payload))
            logger.debug(f"Heartbeat sent: {payload}")
        except Exception as e:
            logger.error(f"Failed to publish heartbeat: {e}")
