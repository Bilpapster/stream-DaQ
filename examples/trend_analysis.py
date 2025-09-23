# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows


def is_stable_trend(trend_value: float) -> bool:
    """
    Assessment function to check if a trend represents stable/flat behavior.
    :param trend_value: the calculated trend slope
    :return: True if trend is stable (slope near zero), False otherwise
    """
    return abs(trend_value) <= 0.1


def is_strong_trend(trend_value: float) -> bool:
    """
    Assessment function to check if a trend is strong (steep slope).
    :param trend_value: the calculated trend slope  
    :return: True if absolute trend slope is greater than 0.5, False otherwise
    """
    return abs(trend_value) > 0.5


# Step 1: Configure Stream DaQ for trend analysis monitoring
daq = StreamDaQ().configure(
    window=Windows.tumbling(5),  # Using larger window for better trend detection
    instance="sensor_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

# Step 2: Define trend-based data quality measures
daq.add(dqm.trend('temperature'), assess=">0.2", name="heating_trend") \
    .add(dqm.trend('pressure'), assess="<-0.3", name="pressure_drop") \
    .add(dqm.trend('vibration'), assess=is_stable_trend, name="vibration_stability") \
    .add(dqm.trend('speed'), assess=is_strong_trend, name="speed_variation") \
    .add(dqm.trend('power_consumption'), assess="[-0.1, 0.1]", name="power_stability") \
    .add(dqm.min('temperature'), assess=">20", name="min_temp") \
    .add(dqm.max('temperature'), assess="<80", name="max_temp") \
    .add(dqm.range_conformance_fraction('temperature', 20, 80), assess=">0.8", name="temp_in_range")

# Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()

# Example Trend Analysis Scenarios:
# 
# 1. Heating Trend Detection (heating_trend):
#    - Monitors if temperature is increasing over time (slope > 0.2)
#    - Useful for detecting equipment overheating
#
# 2. Pressure Drop Monitoring (pressure_drop):
#    - Detects decreasing pressure trends (slope < -0.3)
#    - Important for leak detection or pump failures
#
# 3. Vibration Stability (vibration_stability):
#    - Uses custom function to check if vibration remains stable
#    - Trend slope should be close to zero (±0.1)
#
# 4. Speed Variation Detection (speed_variation):
#    - Identifies strong changes in speed (|slope| > 0.5)
#    - Helps detect mechanical issues
#
# 5. Power Consumption Stability (power_stability):
#    - Uses range assessment to ensure power consumption trend is stable
#    - Slope should be between -0.1 and 0.1