# pip install streamdaq

"""
This example demonstrates how to use Stream DaQ with multiple input sources.
Each source has its own configuration (window, time_column, etc.) and output
includes source traceability via the _source_id column.
"""

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
import time
import pathway as pw


# Define two separate data sources simulating different machines/sensors
machine1_data = [
    {"timestamp": 1, "machine_id": "m1", "temperature": 75.2, "pressure": 1200},
    {"timestamp": 2, "machine_id": "m1", "temperature": 76.1, "pressure": 1205},
    {"timestamp": 3, "machine_id": "m1", "temperature": 75.8, "pressure": 1198},
    {"timestamp": 4, "machine_id": "m1", "temperature": 77.0, "pressure": 1210},
    {"timestamp": 5, "machine_id": "m1", "temperature": 76.5, "pressure": 1202},
]

machine2_data = [
    {"timestamp": 1, "machine_id": "m2", "temperature": 68.5, "pressure": 1180},
    {"timestamp": 2, "machine_id": "m2", "temperature": 69.2, "pressure": 1185},
    {"timestamp": 3, "machine_id": "m2", "temperature": 68.9, "pressure": 1182},
    {"timestamp": 4, "machine_id": "m2", "temperature": 70.1, "pressure": 1190},
    {"timestamp": 5, "machine_id": "m2", "temperature": 69.5, "pressure": 1187},
]


class Machine1Subject(pw.io.python.ConnectorSubject):
    """Connector for machine 1 data stream."""
    def run(self):
        for line in machine1_data:
            self.next(**line)
            time.sleep(0.5)


class Machine2Subject(pw.io.python.ConnectorSubject):
    """Connector for machine 2 data stream."""
    def run(self):
        for line in machine2_data:
            self.next(**line)
            time.sleep(0.5)


class MachineSchema(pw.Schema):
    """Schema for machine data."""
    timestamp: int
    machine_id: str
    temperature: float
    pressure: int


# Create two separate pw.Table sources
source1 = pw.io.python.read(
    Machine1Subject(),
    schema=MachineSchema,
)

source2 = pw.io.python.read(
    Machine2Subject(),
    schema=MachineSchema,
)


def write_to_jsonlines(data: pw.internals.Table) -> None:
    """Write output to jsonlines file."""
    pw.io.jsonlines.write(data, "output_multiple_sources.jsonlines")


# Step 1: Configure Stream DaQ with multiple sources
# Each source has its own configuration and is tracked via source_id
daq = StreamDaQ().configure(
    sources=[
        {
            'source': source1,
            'source_id': 'machine_1',
            'window': Windows.sliding(hop=1, duration=3, origin=0),
            'time_column': 'timestamp',
        },
        {
            'source': source2,
            'source_id': 'machine_2',
            'window': Windows.sliding(hop=1, duration=3, origin=0),
            'time_column': 'timestamp',
        },
    ],
    window=Windows.sliding(hop=1, duration=3, origin=0),  # Default for sources without explicit config
    instance="machine_id",
    time_column="timestamp",
    wait_for_late=1,
    sink_operation=write_to_jsonlines,
)

# Step 2: Define what Data Quality means across all sources
# Output will include _source_id column showing which source each record came from
daq.add(dqm.count('machine_id'), assess=">0", name="count_readings") \
    .add(dqm.mean('temperature'), assess="[65, 80]", name="mean_temp") \
    .add(dqm.min('pressure'), assess=">1100", name="min_pressure") \
    .add(dqm.max('pressure'), assess="<1300", name="max_pressure")

# Step 3: Kick-off monitoring for all sources in a single instance
daq.watch_out()
