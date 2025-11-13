# pip install streamdaq

import random
import time
import pathway as pw
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows, DaQMeasures
from streamdaq.anomaly_detectors.StatisticalDetector import StatisticalDetector


def write_to_jsonlines(data: pw.internals.Table) -> None:
    """
    Utility function to write streaming data to a JSONLINES file.
    :param data: Pathway table containing the streaming data
    """
    pw.io.jsonlines.write(data, "sensor_data_output.jsonlines")


class InputSchema(pw.Schema):
    """
    Schema definition for incoming sensor data streams.
    Defines the structure and types of expected data fields.
    """
    colA: int  # Primary sensor measurement (e.g., temperature, pressure)
    colB: int  # Secondary sensor measurement (e.g., humidity, vibration)
    timestamp: int  # Unix timestamp for temporal ordering


class UserEventsSource(pw.io.python.ConnectorSubject):
    """
    Simulated data source that generates synthetic sensor events with anomalies.
    In production, this would be replaced with real data connectors (Kafka, REST API, etc.).
    """

    def run(self):
        # Define specific timestamps where anomalies will occur
        outlier_timestamps = {28, 32, 53, 56, 62, 78, 81, 98}

        for timestamp in range(1, 100):
            if timestamp in outlier_timestamps:
                # Generate anomalous values (significantly outside normal range)
                colA_value = random.choice([2000, 2500, 3000, -5000, -2000])
            else:
                # Generate normal values within expected range
                colA_value = random.randint(0, 50)

            message = {
                "timestamp": timestamp,
                "colA": colA_value,
                "colB": random.randint(0, 50)
            }
            time.sleep(0.2)  # Simulate real-time streaming delay
            self.next(**message)


# Create the streaming data source
user_events_data = pw.io.python.read(UserEventsSource(), schema=InputSchema)

# Step 1: Initialize Stream DaQ for anomaly detection monitoring
daq = StreamDaQ()

# Step 2: Create a critical monitoring task for events
events_task = daq.new_task("events_task", critical=True)

# Step 3: Configure statistical measures for anomaly detection
# Three approaches for specifying measures:

# Approach 1: Specific measure-column pairs for targeted monitoring
measures = [("min", "colA"), ("max", "colB"), ("mean", "colA")]

# Approach 2: Measure names applied to all numeric columns (simplified)
# measures = ["min", "max", "mean"]

# Approach 3: Measure names with explicit column specification (recommended for production)
# measures = ["min", "max", "mean"]
# columns = ["colA", "colB"]

# Step 4: Configure the Statistical Anomaly Detector
detector = StatisticalDetector(
    buffer_size=10,  # Number of historical windows to maintain for baseline statistics
    warmup_time=2,  # Number of initial windows before anomaly detection starts
    threshold=1.5,  # Z-score threshold for anomaly detection (1.5 = moderate sensitivity)
    top_k=2,  # Number of top anomalies to report per window
    measures=measures  # Statistical measures to compute and monitor
#    columns_profiled=columns  # Columns to apply the measures to
)

# Step 5: Configure the monitoring task
events_task.configure(
    source=user_events_data,
    window=Windows.tumbling(5),  # 5-unit tumbling windows
    time_column="timestamp",  # Column used for temporal windowing
    wait_for_late=1,  # Grace period for late-arriving data
    detector=detector  # Anomaly detection engine
)

# Step 6: Start real-time monitoring and anomaly detection
daq.watch_out()

# Example Anomaly Detection Scenarios:
# 1. Statistical Baseline Monitoring:
#    - Maintains rolling statistics (min, max, mean) over historical windows
#    - Detects when current values deviate significantly from baseline
#
# 2. Multi-Column Analysis:
#    - Monitors both colA and colB simultaneously for comprehensive coverage
#    - Identifies anomalies across multiple sensor dimensions
#
# 3. Adaptive Thresholding:
#    - Uses Z-score based detection with configurable sensitivity
#    - Adapts to changing data patterns over time
#
# 4. Top-K Anomaly Reporting:
#    - Reports the most significant anomalies per window
#    - Reduces noise while highlighting critical deviations
#
# Statistical anomaly detection complements rule-based monitoring by identifying
# subtle patterns and deviations that static thresholds might miss. This approach
# is particularly effective for detecting sensor drift, equipment degradation,
# or unexpected operational changes that manifest as statistical outliers.
#
# Stream DaQ's anomaly detection capabilities provide automated, real-time
# monitoring without requiring manual threshold tuning, making it ideal for
# dynamic environments where normal operating ranges may shift over time.