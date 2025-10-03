# pip install streamdaq

import pathway as pw
from streamdaq import DaQMeasures as dqm
from streamdaq import CompactData, Windows, StreamDaQ

# Configuration constants for compact data structure
FIELDS_COLUMN = "fields"
FIELDS = ["temperature", "humidity", "pressure"]  # simulating IoT sensor measurements
VALUES_COLUMN = "values"
TIMESTAMP_COLUMN = "timestamp"


# We first need to define a data source sending compact data.
# If you already have one, skip this part!
class CompactDataSource(pw.io.python.ConnectorSubject):
    """
    Simulates an IoT sensor network sending compact data format.
    
    Example compact format:
    {
        "timestamp": 1,
        "fields": ["temperature", "humidity", "pressure"],
        "values": [23.5, 65.2, 1013.25]
    }

    vs. traditional native format:
    {"timestamp": 1, "temperature": 23.5, "humidity": 65.2, "pressure": 1013.25}
    """

    def run(self):
        nof_fields = len(FIELDS)
        nof_compact_rows = 5  # how many compact data rows to send in this simulation
        timestamp = value = 0
        for _ in range(nof_compact_rows):
            message = {
                TIMESTAMP_COLUMN: timestamp,
                FIELDS_COLUMN: FIELDS,
                VALUES_COLUMN: [value + i for i in range(nof_fields)]
                # VALUES_COLUMN: [value + i if (value + i) % 5 > 0 else None for i in range(nof_fields)]
                # replace with the above line to make it more spicy by adding a missing reading every five ;)
            }
            value += len(FIELDS)
            timestamp += 1
            self.next(**message)


# Define schema for the compact data structure
schema_dict = {
    TIMESTAMP_COLUMN: int,
    FIELDS_COLUMN: list[str],
    VALUES_COLUMN: list[int | None],  # Supports missing values (None) for real-world scenarios
}
schema = pw.schema_from_dict(schema_dict)

# Create the compact data stream (simulating IoT sensor network)
compact_data_stream = pw.io.python.read(
    CompactDataSource(),
    schema=schema,
)

print("The initial data source sends compact data, like this:")
pw.debug.compute_and_print(compact_data_stream)

# If you already have a compact data source, your job starts here!

# Step 1: Configure Stream DaQ for compact data monitoring
# Stream DaQ automatically handles the transformation from compact to native format,
# eliminating the need for manual data preprocessing that would typically require:
# - Unpacking compact rows into individual field records
# - Handling missing values and data type conversions
# - Managing temporal alignment across different fields
daq = StreamDaQ().configure(
    window=Windows.sliding(duration=3, hop=1, origin=0),  # 3-second sliding window with 1-second hop
    source=compact_data_stream,
    time_column=TIMESTAMP_COLUMN,
    # Just define how your compact data is structured; Stream DaQ takes care of all the rest!
    # This CompactData configuration tells Stream DaQ how to interpret your format
    compact_data=CompactData()
    .with_fields_column(FIELDS_COLUMN)
    .with_values_column(VALUES_COLUMN)
    .with_values_dtype(int),
)

# Step 2: Define data quality measures for IoT sensor monitoring
# Notice how we can directly reference individual fields (temperature, humidity, pressure)
# even though they arrive in compact format - Stream DaQ handles the unpacking automatically!
daq.add(dqm.count("pressure"), name="readings") \
    .add(dqm.missing_count("temperature") 
         + dqm.missing_count("pressure") 
         + dqm.missing_count("humidity"), # Measures the total missing readings per window in all fields
        assess="<2",  # We can tolerate at most one missing reading per window
        name="missing_readings",
    ). \
    add(dqm.is_frozen("humidity"), name="frozen_humidity_sensor")  # Detect stuck humidity sensor

# Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py


# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()

# IoT Compact Data Monitoring Benefits:
#
# 1. Bandwidth Efficiency:
#    - Compact format reduces network traffic by ~60% compared to individual field transmissions
#    - Critical for battery-powered sensors with limited connectivity
#
# 2. Automatic Transformation:
#    - Stream DaQ internally converts compact data to native format for quality analysis
#    - No manual preprocessing required - just specify the compact data structure
#    - Handles missing values, data types, and temporal alignment automatically
#
# 3. Real-World IoT Scenarios:
#    - Environmental monitoring stations (temperature, humidity, pressure)
#    - Industrial sensor networks (vibration, temperature, speed)
#    - Smart building systems (occupancy, air quality, energy usage)
#    - Vehicle telemetry (GPS, speed, fuel consumption, engine metrics)
#
# 4. Quality Monitoring Without Complexity:
#    - Apply the same quality measures as native data streams
#    - Detect sensor failures, missing readings, and data anomalies
#    - Monitor trends and patterns across multiple sensor types simultaneously
#
# Stream DaQ's compact data handling eliminates the typical IoT data preprocessing
# pipeline, allowing you to focus on defining meaningful quality measures rather
# than data transformation logic. This is especially valuable in resource-constrained
# environments where development time and computational efficiency are critical!
#
# 📚 Learn More:
# - Comprehensive compact data documentation: docs/examples/advanced-examples.rst
# - Conceptual background: docs/concepts/compact-vs-native-data.rst
