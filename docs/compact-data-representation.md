# Compact Data Representation for MQTT Input

StreamDaQ now supports compact data representation for MQTT and other inputs, providing automatic transformation from compact format to native representation. This feature allows users to work with efficiently transmitted data while maintaining the same StreamDaQ experience.

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
2. **Transparent**: Users work with native field names as if data was originally in native format  
3. **Automatic**: No manual transformation required from the user
4. **Consistent**: Same StreamDaQ experience regardless of input format

## Usage

### Basic Setup

```python
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows, mqtt_connectors

# Define your expected field names
field_names = ["temperature", "humidity", "pressure"]

# Create MQTT source with compact support
mqtt_source = mqtt_connectors.create_mqtt_compact_streamdaq_source(
    field_names=field_names,
    broker_host="your-mqtt-broker.com",
    topic="sensors/data"
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

### Custom Connector

You can also use compact representation with custom connectors:

```python
import pathway as pw
from streamdaq.utils import transform_compact_to_native

class CompactSchema(pw.Schema):
    fields: list[str] 
    values: list[float]
    timestamp: int

# Your custom connector
compact_table = pw.io.python.read(your_connector, schema=CompactSchema)

# Use with StreamDaQ
daq = StreamDaQ().configure(
    window=Windows.sliding(duration=30, hop=10),
    time_column="timestamp", 
    source=compact_table,
    compact_fields=["sensor1", "sensor2", "sensor3"]
)
```

## MQTT Integration

The `mqtt_connectors` module provides ready-to-use MQTT connectivity:

```python
from streamdaq import mqtt_connectors

# Create MQTT source
mqtt_source = mqtt_connectors.create_mqtt_compact_streamdaq_source(
    field_names=["temp", "humid", "press"],
    broker_host="mqtt.example.com",
    broker_port=1883,
    topic="iot/sensors",
    username="user",
    password="pass",
    qos=1
)
```

## Requirements and Assumptions

1. **Field Names Known in Advance**: You must provide the expected field names when configuring StreamDaQ
2. **Consistent Schema**: The data schema (names and number of fields) should not change during execution
3. **Ordered Values**: Values in the `values` array must correspond to the order of field names
4. **Valid Format**: Input data must have both `fields` and `values` keys with matching array lengths

## Implementation Details

The compact-to-native transformation:

1. **Automatic**: Triggered when `compact_fields` parameter is provided
2. **Internal**: Completely transparent to the user
3. **Efficient**: Uses Pathway UDFs for optimal performance
4. **Type-Safe**: Maintains proper data types throughout the transformation

## Error Handling

- Invalid compact format will cause processing warnings
- Missing or mismatched field/value arrays are handled gracefully
- Out-of-bounds field access returns sensible defaults (0.0)

## Example MQTT Message Formats

### Sensor Data
```json
{
  "fields": ["temperature", "humidity", "light_level", "motion"],
  "values": [22.5, 45.8, 350.0, 0.0],
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

### IoT Metrics
```json
{
  "fields": ["cpu_usage", "memory_usage", "disk_usage", "network_io"],
  "values": [75.2, 68.1, 45.0, 1024.5],
  "timestamp": 1758745400
}
```

This feature enables StreamDaQ to work seamlessly with space-efficient data transmission while maintaining full data quality monitoring capabilities.