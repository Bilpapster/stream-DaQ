"""
MQTT connectors for Stream DaQ with support for compact data representation.

This module provides MQTT connectivity with automatic compact-to-native data transformation.
"""

import json
import time
from typing import Any, Dict, List, Optional
import pathway as pw


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

        Note: This is a simplified implementation for demonstration.
        In production, you would use a real MQTT client library like paho-mqtt.
        """
        try:
            # For now, we'll simulate MQTT messages
            # In real implementation, this would use paho-mqtt or similar
            self._run_simulation()
        except Exception as e:
            print(f"Error in MQTT connector: {e}")

    def _run_simulation(self):
        """
        Simulate MQTT messages for demonstration purposes.
        In production, this would be replaced with actual MQTT client code.
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

        This method would be called by the MQTT client when a message is received.
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

        except Exception as e:
            print(f"Error processing MQTT message: {e}")

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


def create_mqtt_compact_source(
    field_names: List[str],
    broker_host: str = "localhost",
    broker_port: int = 1883,
    topic: str = "streamdaq/data",
    username: Optional[str] = None,
    password: Optional[str] = None,
    qos: int = 0,
    timeout: int = 60,
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

    Returns:
        Pathway table with compact data that can be transformed by StreamDaQ
    """
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
    )
