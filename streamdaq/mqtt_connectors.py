"""
MQTT connectors for Stream DaQ with support for compact data representation.

This module provides MQTT connectivity with automatic compact-to-native data transformation.
"""

import json
import time
import threading
from typing import Any, Dict, List, Optional
import pathway as pw

try:
    import paho.mqtt.client as mqtt
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False


class MqttCompactConnectorSubject(pw.io.python.ConnectorSubject):
    """
    MQTT connector subject that provides compact data representation support.

    This connector expects MQTT messages in the format:
    {
        "fields": ["field1", "field2", "field3", ...],
        "values": [value1, value2, value3, ...]
    }

    The connector will emit the data as-is, and the StreamDaQ system will
    handle the compact-to-native transformation internally.
    """

    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        topic: str = "streamdaq/data",
        username: Optional[str] = None,
        password: Optional[str] = None,
        qos: int = 0,
        timeout: int = 60,
    ):
        """
        Initialize the MQTT compact connector.

        Args:
            broker_host: MQTT broker hostname or IP
            broker_port: MQTT broker port
            topic: MQTT topic to subscribe to
            username: MQTT username (optional)
            password: MQTT password (optional)
            qos: Quality of Service level (0, 1, or 2)
            timeout: Connection timeout in seconds
        """
        super().__init__()
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.username = username
        self.password = password
        self.qos = qos
        self.timeout = timeout
        self._client = None

    def run(self):
        """
        Main run method that connects to MQTT and processes messages.
        
        Uses paho-mqtt client for production-ready MQTT connectivity.
        Falls back to simulation mode if paho-mqtt is not available.
        """
        if not PAHO_AVAILABLE:
            print("Warning: paho-mqtt not available, falling back to simulation mode")
            self._run_simulation()
            return
            
        try:
            self._run_paho_mqtt()
        except Exception as e:
            print(f"Error in MQTT connector: {e}")
            # Fallback to simulation mode for development/testing
            print("Falling back to simulation mode")
            self._run_simulation()

    def _run_paho_mqtt(self):
        """
        Production MQTT implementation using paho-mqtt client.
        """
        # Create MQTT client
        self._client = mqtt.Client(client_id=f"streamdaq_{int(time.time())}")
        
        # Set callbacks
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_mqtt_message
        self._client.on_disconnect = self._on_disconnect
        
        # Set authentication if provided
        if self.username and self.password:
            self._client.username_pw_set(self.username, self.password)
        
        # Connect to broker
        print(f"Connecting to MQTT broker {self.broker_host}:{self.broker_port}")
        self._client.connect(self.broker_host, self.broker_port, self.timeout)
        
        # Start the client loop in a separate thread
        self._client.loop_start()
        
        # Keep the connector running
        self._keep_running = True
        while self._keep_running:
            time.sleep(1)
            
        # Clean up
        self._client.loop_stop()
        self._client.disconnect()

    def _on_connect(self, client, userdata, flags, rc):
        """
        Callback for when the client receives a CONNACK response from the server.
        """
        if rc == 0:
            print(f"Connected to MQTT broker successfully")
            # Subscribe to the topic
            client.subscribe(self.topic, self.qos)
            print(f"Subscribed to topic: {self.topic}")
        else:
            print(f"Failed to connect to MQTT broker, return code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """
        Callback for when the client disconnects from the broker.
        """
        if rc != 0:
            print("Unexpected disconnection from MQTT broker")
        else:
            print("Disconnected from MQTT broker")
            
        # Stop the main loop
        self._keep_running = False

    def _run_simulation(self):
        """
        Simulate MQTT messages for demonstration/testing purposes.
        This is used when paho-mqtt is not available or as a fallback.
        """
        import random

        # Simulate receiving compact MQTT messages
        field_names = ["temperature", "humidity", "pressure"]

        # Generate 10 simulated messages
        for i in range(10):
            message_data = {
                "fields": field_names,
                "values": [
                    random.uniform(20.0, 30.0),  # temperature
                    random.uniform(40.0, 80.0),  # humidity
                    random.uniform(1000.0, 1020.0),  # pressure
                ],
                "timestamp": int(time.time()) + i
            }

            # Emit the message
            self.next(**message_data)
            time.sleep(1)  # 1 second between messages

    def _on_mqtt_message(self, client, userdata, message):
        """
        Callback for handling MQTT messages.

        This method is called by the paho MQTT client when a message is received.
        """
        try:
            # Decode the message payload
            payload = message.payload.decode('utf-8')
            data = json.loads(payload)

            # Validate the message format
            if not self._validate_compact_message(data):
                print(f"Invalid compact message format: {data}")
                return

            # Add timestamp if not present
            if 'timestamp' not in data:
                data['timestamp'] = int(time.time())

            # Emit the data to Pathway
            self.next(**data)

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON message: {e}")
        except Exception as e:
            print(f"Error processing MQTT message: {e}")

    def stop(self):
        """
        Stop the MQTT connector gracefully.
        """
        if hasattr(self, '_keep_running'):
            self._keep_running = False
        if self._client:
            self._client.disconnect()

    def _validate_compact_message(self, data: Dict[str, Any]) -> bool:
        """
        Validate that the message has the correct compact format.

        Args:
            data: The message data to validate

        Returns:
            True if valid, False otherwise
        """
        if not isinstance(data, dict):
            return False

        if 'fields' not in data or 'values' not in data:
            return False

        if not isinstance(data['fields'], list) or not isinstance(data['values'], list):
            return False

        if len(data['fields']) != len(data['values']):
            return False

        return True


class MqttCompactSchema(pw.Schema):
    """Schema for compact MQTT messages."""
    fields: list[str]
    values: list[float]
    timestamp: int


class ProductionMqttCompactConnectorSubject(pw.io.python.ConnectorSubject):
    """
    Production-ready MQTT connector subject using paho-mqtt client.
    
    This connector uses the paho-mqtt library to connect to real MQTT brokers
    and process compact data representation messages.
    """

    def __init__(
        self,
        field_names: List[str],
        broker_host: str = "localhost",
        broker_port: int = 1883,
        topic: str = "streamdaq/data",
        username: Optional[str] = None,
        password: Optional[str] = None,
        qos: int = 0,
        timeout: int = 60,
        client_id: Optional[str] = None,
        use_tls: bool = False,
        ca_certs: Optional[str] = None,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
    ):
        """
        Initialize the production MQTT compact connector.

        Args:
            field_names: Expected field names in the compact representation
            broker_host: MQTT broker hostname or IP
            broker_port: MQTT broker port
            topic: MQTT topic to subscribe to
            username: MQTT username (optional)
            password: MQTT password (optional)
            qos: Quality of Service level (0, 1, or 2)
            timeout: Connection timeout in seconds
            client_id: MQTT client ID (auto-generated if not provided)
            use_tls: Whether to use TLS encryption
            ca_certs: Path to CA certificates file for TLS
            certfile: Path to client certificate file for TLS
            keyfile: Path to client private key file for TLS
        """
        super().__init__()
        
        if not PAHO_AVAILABLE:
            raise ImportError("paho-mqtt is required for production MQTT connector. Install with: pip install paho-mqtt")
            
        self.field_names = field_names
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.topic = topic
        self.username = username
        self.password = password
        self.qos = qos
        self.timeout = timeout
        self.client_id = client_id or f"streamdaq_prod_{int(time.time())}"
        self.use_tls = use_tls
        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile = keyfile
        self._client = None
        self._keep_running = False

    def run(self):
        """
        Main run method that connects to MQTT and processes messages.
        """
        try:
            # Create MQTT client
            self._client = mqtt.Client(client_id=self.client_id)
            
            # Configure TLS if requested
            if self.use_tls:
                self._client.tls_set(ca_certs=self.ca_certs, certfile=self.certfile, keyfile=self.keyfile)
            
            # Set callbacks
            self._client.on_connect = self._on_connect
            self._client.on_message = self._on_mqtt_message
            self._client.on_disconnect = self._on_disconnect
            self._client.on_log = self._on_log
            
            # Set authentication if provided
            if self.username and self.password:
                self._client.username_pw_set(self.username, self.password)
            
            # Connect to broker
            print(f"[StreamDaQ MQTT] Connecting to {self.broker_host}:{self.broker_port}")
            self._client.connect(self.broker_host, self.broker_port, self.timeout)
            
            # Start the client loop
            self._keep_running = True
            self._client.loop_forever()
            
        except Exception as e:
            print(f"Error in production MQTT connector: {e}")
            raise

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for successful MQTT connection."""
        if rc == 0:
            print(f"[StreamDaQ MQTT] Connected successfully")
            client.subscribe(self.topic, self.qos)
            print(f"[StreamDaQ MQTT] Subscribed to topic: {self.topic}")
        else:
            print(f"[StreamDaQ MQTT] Failed to connect, return code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection."""
        if rc != 0:
            print("[StreamDaQ MQTT] Unexpected disconnection")
        else:
            print("[StreamDaQ MQTT] Disconnected")

    def _on_log(self, client, userdata, level, buf):
        """Callback for MQTT client logging."""
        # Only log important messages
        if level <= mqtt.MQTT_LOG_WARNING:
            print(f"[StreamDaQ MQTT] {buf}")

    def _on_mqtt_message(self, client, userdata, message):
        """Process incoming MQTT messages."""
        try:
            payload = message.payload.decode('utf-8')
            data = json.loads(payload)

            # Validate compact message format
            if not self._validate_compact_message(data):
                print(f"[StreamDaQ MQTT] Invalid message format: {data}")
                return

            # Validate field names match expectations
            if data.get('fields') != self.field_names:
                print(f"[StreamDaQ MQTT] Field mismatch. Expected: {self.field_names}, Got: {data.get('fields')}")
                return

            # Add timestamp if not present
            if 'timestamp' not in data:
                data['timestamp'] = int(time.time())

            # Emit to Pathway
            self.next(**data)

        except json.JSONDecodeError as e:
            print(f"[StreamDaQ MQTT] JSON decode error: {e}")
        except Exception as e:
            print(f"[StreamDaQ MQTT] Message processing error: {e}")

    def _validate_compact_message(self, data: Dict[str, Any]) -> bool:
        """Validate compact message format."""
        if not isinstance(data, dict):
            return False
        
        required_fields = ['fields', 'values']
        if not all(field in data for field in required_fields):
            return False
        
        if not isinstance(data['fields'], list) or not isinstance(data['values'], list):
            return False
        
        if len(data['fields']) != len(data['values']):
            return False
        
        return True

    def stop(self):
        """Stop the MQTT connector gracefully."""
        self._keep_running = False
        if self._client:
            self._client.disconnect()


def create_mqtt_compact_source(
    field_names: List[str],
    broker_host: str = "localhost",
    broker_port: int = 1883,
    topic: str = "streamdaq/data",
    username: Optional[str] = None,
    password: Optional[str] = None,
    qos: int = 0,
    timeout: int = 60,
    use_production: bool = True,
    client_id: Optional[str] = None,
    use_tls: bool = False,
    ca_certs: Optional[str] = None,
    certfile: Optional[str] = None,
    keyfile: Optional[str] = None,
) -> pw.Table:
    """
    Create an MQTT source with compact data representation support.

    Args:
        field_names: Expected field names in the compact representation
        broker_host: MQTT broker hostname or IP
        broker_port: MQTT broker port
        topic: MQTT topic to subscribe to
        username: MQTT username (optional)
        password: MQTT password (optional)
        qos: Quality of Service level (0, 1, or 2)
        timeout: Connection timeout in seconds
        use_production: Use production paho-mqtt client (default: True)
        client_id: MQTT client ID (auto-generated if not provided)
        use_tls: Whether to use TLS encryption
        ca_certs: Path to CA certificates file for TLS
        certfile: Path to client certificate file for TLS
        keyfile: Path to client private key file for TLS

    Returns:
        Pathway table with compact data that can be transformed by StreamDaQ
    """
    if use_production and PAHO_AVAILABLE:
        connector = ProductionMqttCompactConnectorSubject(
            field_names=field_names,
            broker_host=broker_host,
            broker_port=broker_port,
            topic=topic,
            username=username,
            password=password,
            qos=qos,
            timeout=timeout,
            client_id=client_id,
            use_tls=use_tls,
            ca_certs=ca_certs,
            certfile=certfile,
            keyfile=keyfile,
        )
    else:
        # Fallback to demo/simulation connector
        if use_production:
            print("Warning: paho-mqtt not available, using simulation connector")
        connector = MqttCompactConnectorSubject(
            broker_host=broker_host,
            broker_port=broker_port,
            topic=topic,
            username=username,
            password=password,
            qos=qos,
            timeout=timeout,
        )

    # Create the compact data table
    compact_table = pw.io.python.read(connector, schema=MqttCompactSchema)
    return compact_table


def create_mqtt_compact_streamdaq_source(
    field_names: List[str],
    broker_host: str = "localhost",
    broker_port: int = 1883,
    topic: str = "streamdaq/data",
    username: Optional[str] = None,
    password: Optional[str] = None,
    qos: int = 0,
    timeout: int = 60,
    use_production: bool = True,
    client_id: Optional[str] = None,
    use_tls: bool = False,
    ca_certs: Optional[str] = None,
    certfile: Optional[str] = None,
    keyfile: Optional[str] = None,
) -> pw.Table:
    """
    Create an MQTT source that's ready to use with StreamDaQ's compact field support.

    This is a convenience function that creates the MQTT source and returns it
    in a format that works seamlessly with StreamDaQ's compact_fields parameter.

    Args:
        field_names: Expected field names in the compact representation
        broker_host: MQTT broker hostname or IP
        broker_port: MQTT broker port
        topic: MQTT topic to subscribe to
        username: MQTT username (optional)
        password: MQTT password (optional)
        qos: Quality of Service level (0, 1, or 2)
        timeout: Connection timeout in seconds
        use_production: Use production paho-mqtt client (default: True)
        client_id: MQTT client ID (auto-generated if not provided)
        use_tls: Whether to use TLS encryption
        ca_certs: Path to CA certificates file for TLS
        certfile: Path to client certificate file for TLS
        keyfile: Path to client private key file for TLS

    Returns:
        Pathway table ready for use with StreamDaQ
    """
    return create_mqtt_compact_source(
        field_names=field_names,
        broker_host=broker_host,
        broker_port=broker_port,
        topic=topic,
        username=username,
        password=password,
        qos=qos,
        timeout=timeout,
        use_production=use_production,
        client_id=client_id,
        use_tls=use_tls,
        ca_certs=ca_certs,
        certfile=certfile,
        keyfile=keyfile,
    )


# Legacy functions for backward compatibility
create_production_mqtt_compact_source = create_mqtt_compact_source
create_production_mqtt_compact_streamdaq_source = create_mqtt_compact_streamdaq_source
