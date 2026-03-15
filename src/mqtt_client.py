"""
MQTT Client for AWS IoT Core with message routing support.

Connects to AWS IoT Core, subscribes to a single request topic,
and publishes responses to a single response topic.
"""

import json
import logging
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder

from src.message_router import MessageRouter

logger = logging.getLogger(__name__)


class MqttClient:
    """MQTT Client with single topic subscribe/publish model"""

    def __init__(self, config):
        self.config = config
        self.connection = None
        self.router = None
        self._waiting_for = {}  # request_id -> (Event, container)

    def connect(self):
        """Establish connection to AWS IoT Core"""
        event_loop_group = io.EventLoopGroup(1)
        host_resolver = io.DefaultHostResolver(event_loop_group)
        client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

        self.connection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.config.endpoint,
            port=self.config.port,
            cert_filepath=self.config.cert_path,
            pri_key_filepath=self.config.private_key_path,
            ca_filepath=self.config.root_ca_path,
            client_bootstrap=client_bootstrap,
            client_id=self.config.client_id,
            clean_session=False,
            keep_alive_secs=30,
            on_connection_interrupted=self._on_connection_interrupted,
            on_connection_resumed=self._on_connection_resumed,
        )

        logger.info(f"Connecting to {self.config.endpoint}...")
        connect_future = self.connection.connect()
        connect_result = connect_future.result()
        logger.info(f"Connected! Session present: {connect_result['session_present']}")

        # Initialize message router with self to allow access to publish method
        self.router = MessageRouter(self)

    def subscribe(self):
        """Subscribe to the configured request topic"""
        topic = self.config.subscribe_topic
        logger.info(f"Subscribing to: {topic}")

        subscribe_future, _ = self.connection.subscribe(
            topic=topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=self._on_message_received,
        )
        subscribe_result = subscribe_future.result()
        logger.info(f"Subscribed to {topic} with QoS: {subscribe_result['qos']}")

    def subscribe_to_topic(self, topic: str):
        """Subscribe to an arbitrary topic"""
        logger.info(f"Subscribing to extra topic: {topic}")
        subscribe_future, _ = self.connection.subscribe(
            topic=topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=self._on_message_received,
        )
        subscribe_future.result()

    def wait_for_message(self, topic: str, request_id: str, timeout: int = 30) -> dict:
        """
        Wait for a message with a specific requestId on a topic.
        Robust version that logs progress and handles connection issues.
        Note: Caller should subscribe to the topic BEFORE calling this method.
        """
        import threading
        import time

        event = threading.Event()
        container = {}
        self._waiting_for[request_id] = (event, container)

        start_time = time.time()
        logger.info(
            f"⏳ Waiting for message (reqId={request_id}) on {topic} for {timeout}s..."
        )

        try:
            # Wait in chunks to log progress or check connection
            waited = 0
            chunk = 5  # Check every 5 seconds

            while waited < timeout:
                if event.wait(chunk):
                    logger.info(
                        f"✅ Received response for {request_id} after {time.time() - start_time:.1f}s"
                    )
                    return container.get("payload")

                waited += chunk
                remaining = timeout - waited
                if remaining > 0:
                    logger.info(
                        f"   ...still waiting ({remaining}s left). Connection valid? {bool(self.connection)}"
                    )

            logger.warning(
                f"❌ Timeout waiting for message {request_id} after {timeout}s"
            )
            return None

        finally:
            self._waiting_for.pop(request_id, None)
            # self.connection.unsubscribe(topic)

    def publish(self, topic: str, payload: bytes or str, qos=mqtt.QoS.AT_LEAST_ONCE):
        """Generic publish method"""
        if isinstance(payload, str):
            payload = payload.encode("utf-8")

        payload_size = len(payload)
        logger.debug(f"Publishing to '{topic}' (Payload size: {payload_size} bytes)")

        # Warn if approaching AWS IoT limit (128KB = 131072 bytes)
        if payload_size > 120000:
            logger.warning(
                f"⚠️ Payload size ({payload_size} bytes) is close to AWS IoT limit (128KB)!"
            )

        publish_future, _ = self.connection.publish(
            topic=topic,
            payload=payload,
            qos=qos,
        )
        return publish_future

    def _on_message_received(self, topic: str, payload: bytes, **kwargs):
        """Handle incoming messages by routing them"""
        # PRINT ENTIRE RECEIVED MESSAGE
        print(f"\n---> [RECEIVED RAW] Topic: {topic}")
        msg = None
        try:
            msg = json.loads(payload.decode("utf-8"))
            print(json.dumps(msg, indent=2))
        except:
            print(payload.decode("utf-8"))
        print("---> [END RECEIVED]\n")

        logger.info(f"Received message on '{topic}'")

        # Check if any thread is waiting for this message
        if msg:
            req_id = msg.get("requestId")
            if req_id and req_id in self._waiting_for:
                event, container = self._waiting_for[req_id]
                container["payload"] = msg
                event.set()

        if self.router:
            self.router.route_message(payload)
        else:
            logger.error("Router not initialized!")

    def _publish_response(self, message: dict):
        """Publish a response message to the response topic"""
        topic = self.config.publish_topic

        # PRINT ENTIRE SENT MESSAGE
        print(f"\n<--- [SENT RAW] Topic: {topic}")
        print(json.dumps(message, indent=2))
        print("<--- [END SENT]\n")

        logger.debug(f"Publishing to '{topic}': {message}")

        self.publish(topic, json.dumps(message))
        logger.info(
            f"Published response for message_id: {message.get('payload', {}).get('message_id', 'unknown')}"
        )

    def _on_connection_interrupted(self, connection, error, **kwargs):
        logger.warning(f"Connection interrupted: {error}")

    def _on_connection_resumed(
        self, connection, return_code, session_present, **kwargs
    ):
        logger.info(f"Connection resumed. Return code: {return_code}")
        # Re-subscribe on reconnection
        if not session_present:
            logger.info("Session not present, re-subscribing...")
            self.subscribe()

    def disconnect(self):
        """Disconnect from AWS IoT Core"""
        if self.connection:
            disconnect_future = self.connection.disconnect()
            disconnect_future.result()
            logger.info("Disconnected")
