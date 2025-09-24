# Compact Data Representation for MQTT Input

StreamDaQ now supports compact data representation for MQTT and other inputs, providing automatic transformation from compact format to native representation. This feature includes **production-ready MQTT connectivity** using the `paho-mqtt` client library.

## What is Compact Data Representation?

### Compact Format
Data is transmitted as:
```json
{
  "fields": ["temperature", "humidity", "pressure"],
  "values": [23.5, 67.2, 1013.25],
  "timestamp": 1234567890
}
```

### Native Format (Internal)
StreamDaQ automatically transforms this to:
```
| timestamp  | temperature | humidity | pressure |
|------------|-------------|----------|----------|
| 1234567890 | 23.5        | 67.2     | 1013.25  |
```

## Key Benefits

1. **Bandwidth Efficiency**: Reduces data transmission size by avoiding repeated field names
2. **Production Ready**: Uses `paho-mqtt` for reliable MQTT connectivity
3. **Transparent**: Users work with native field names as if data was originally in native format  
4. **Automatic**: No manual transformation required from the user
5. **Secure**: Supports TLS/SSL encryption and authentication
6. **Robust**: Includes error handling, reconnection, and graceful fallbacks

## Production MQTT Setup

### Basic Production Usage

```python
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows, mqtt_connectors

# Define your expected field names
field_names = ["temperature", "humidity", "pressure"]

# Create production MQTT source
mqtt_source = mqtt_connectors.create_mqtt_compact_streamdaq_source(
    field_names=field_names,
    broker_host="your-mqtt-broker.com",
    broker_port=1883,
    topic="sensors/data",
    username="your_username",
    password="your_password",
    use_production=True,  # Uses paho-mqtt client (default)
    client_id="streamdaq_client_001",
    qos=1,
    timeout=60
)

# Configure StreamDaQ with compact fields
daq = StreamDaQ().configure(
    window=Windows.tumbling(60),  # 1-minute windows
    time_column="timestamp",
    source=mqtt_source,
    compact_fields=field_names,  # Enable compact-to-native transformation
    wait_for_late=5,
)

# Use native field names directly in your quality measures
daq.add(dqm.count('temperature'), assess=">0", name="temp_count") \
   .add(dqm.mean('temperature'), assess="[15, 35]", name="avg_temp") \
   .add(dqm.max('humidity'), assess="<=100", name="max_humidity")

# Start monitoring
daq.watch_out()
```

### Secure MQTT with TLS

```python
# Production setup with TLS encryption
mqtt_source = mqtt_connectors.create_mqtt_compact_streamdaq_source(
    field_names=["sensor1", "sensor2", "sensor3"],
    broker_host="secure-mqtt-broker.com",
    broker_port=8883,  # TLS port
    topic="production/sensors",
    username="prod_user",
    password="secure_password",
    use_tls=True,
    ca_certs="/path/to/ca-certificates.crt",
    certfile="/path/to/client.crt",  # Optional client cert
    keyfile="/path/to/client.key",   # Optional client key
    qos=2,  # Highest QoS for critical data
    client_id="streamdaq_production"
)
```

## MQTT Features

### Production Features
- ✅ **Real MQTT Connectivity**: Uses `paho-mqtt` library for production-grade MQTT communication
- ✅ **TLS/SSL Support**: Secure encrypted connections with certificate validation
- ✅ **Authentication**: Username/password and certificate-based authentication
- ✅ **Quality of Service**: Full QoS 0, 1, 2 support for message delivery guarantees
- ✅ **Error Handling**: Robust error handling with logging and fallback mechanisms
- ✅ **Reconnection**: Automatic reconnection handling for network interruptions
- ✅ **Message Validation**: Validates compact message format and field consistency

### Development Features
- 🔧 **Simulation Mode**: Built-in simulation for development and testing
- 🔧 **Fallback Support**: Graceful fallback when paho-mqtt is not available
- 🔧 **Debug Logging**: Comprehensive logging for troubleshooting

## Installation

The production MQTT connector requires the `paho-mqtt` library:

```bash
pip install streamdaq  # Includes paho-mqtt dependency
```

Or to install manually:
```bash  
pip install paho-mqtt>=1.6.0
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `field_names` | `List[str]` | Required | Expected field names in compact representation |
| `broker_host` | `str` | `"localhost"` | MQTT broker hostname or IP |
| `broker_port` | `int` | `1883` | MQTT broker port (8883 for TLS) |
| `topic` | `str` | `"streamdaq/data"` | MQTT topic to subscribe to |
| `username` | `str` | `None` | MQTT username (optional) |
| `password` | `str` | `None` | MQTT password (optional) |
| `use_production` | `bool` | `True` | Use paho-mqtt client vs simulation |
| `client_id` | `str` | Auto-generated | Unique MQTT client identifier |
| `qos` | `int` | `0` | Quality of Service level (0, 1, or 2) |
| `timeout` | `int` | `60` | Connection timeout in seconds |
| `use_tls` | `bool` | `False` | Enable TLS/SSL encryption |
| `ca_certs` | `str` | `None` | Path to CA certificates file |
| `certfile` | `str` | `None` | Path to client certificate file |
| `keyfile` | `str` | `None` | Path to client private key file |

## Message Format Validation

The production connector validates incoming messages:

```python
# ✅ Valid compact message
{
    "fields": ["temp", "humid", "press"],
    "values": [22.5, 65.0, 1013.25],
    "timestamp": 1234567890  # Optional, auto-added if missing
}

# ❌ Invalid messages are rejected with logging
{
    "fields": ["temp", "humid"],
    "values": [22.5, 65.0, 1013.25]  # Length mismatch
}
```

## Error Handling

The production connector includes comprehensive error handling:

```python
# Connection errors
[StreamDaQ MQTT] Failed to connect, return code 5

# Message validation errors  
[StreamDaQ MQTT] Invalid message format: {...}

# Field validation errors
[StreamDaQ MQTT] Field mismatch. Expected: [...], Got: [...]

# JSON parsing errors
[StreamDaQ MQTT] JSON decode error: Expecting ',' delimiter
```

## Development and Testing

For development and testing without a real MQTT broker:

```python
# Use simulation mode
mqtt_source = mqtt_connectors.create_mqtt_compact_streamdaq_source(
    field_names=["temp", "humidity"],
    use_production=False  # Uses built-in simulation
)
```

## Example MQTT Messages

### IoT Sensor Data
```json
{
  "fields": ["temperature", "humidity", "light", "motion"],
  "values": [22.5, 45.8, 350, 0],
  "timestamp": 1758745400
}
```

### Industrial Monitoring  
```json
{
  "fields": ["pressure", "flow_rate", "temperature", "vibration"],
  "values": [85.2, 1250.5, 78.3, 0.15],
  "timestamp": 1758745400
}
```

### Financial Data
```json
{
  "fields": ["price", "volume", "bid", "ask"],
  "values": [150.25, 1000, 150.20, 150.30],
  "timestamp": 1758745400
}
```

This production-ready implementation enables StreamDaQ to work with real MQTT infrastructures while maintaining the simplicity of compact data representation and automatic quality monitoring.