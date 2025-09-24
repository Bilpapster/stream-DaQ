# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows


def is_trending_up(trend_value: float) -> bool:
    """
    Assessment function to check if trend is significantly positive.
    :param trend_value: The calculated trend slope value.
    :return: True if trend is significantly upward (> 0.1), False otherwise.
    """
    return trend_value > 0.1


def is_stable_trend(trend_value: float) -> bool:
    """
    Assessment function to check if trend is stable (near zero).
    :param trend_value: The calculated trend slope value.
    :return: True if trend is stable (-0.1 <= trend <= 0.1), False otherwise.
    """
    return -0.1 <= trend_value <= 0.1


# Configure StreamDaQ with trend analysis
daq = StreamDaQ().configure(
    window=Windows.tumbling(5),  # Analyze trends over 5 data points
    instance="user_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

# Define trend-based data quality measurements
daq.add(dqm.trend('stock_price'), assess=">0", name="price_rising") \
    .add(dqm.trend('stock_price'), assess="<-0.5", name="price_falling_fast") \
    .add(dqm.trend('stock_price'), assess=is_stable_trend, name="price_stable") \
    .add(dqm.trend('temperature'), assess="[-0.2, 0.2]", name="temp_trend_stable") \
    .add(dqm.trend('cpu_usage'), assess=is_trending_up, name="cpu_usage_increasing") \
    .add(dqm.trend('memory_usage'), assess=">=1.0", name="memory_spike_trend") \
    .add(dqm.max('stock_price'), assess=">100", name="high_price") \
    .add(dqm.mean('stock_price', precision=2), assess="[80, 120]", name="normal_price_range")

# Trend analysis allows you to monitor:
# 1. Rising trends: detect when values are consistently increasing
# 2. Falling trends: identify when values are declining rapidly  
# 3. Stable trends: ensure values remain relatively constant
# 4. Volatility: detect sudden changes in trend direction

print("Stream DaQ Trend Analysis Demo")
print("==============================")
print("Monitoring the following trend-based data quality measures:")
print("• price_rising: Detects if stock price has positive trend")
print("• price_falling_fast: Alerts when price drops with slope < -0.5") 
print("• price_stable: Checks if price trend is stable (±0.1)")
print("• temp_trend_stable: Monitors temperature trend stability")
print("• cpu_usage_increasing: Detects increasing CPU usage patterns")
print("• memory_spike_trend: Alerts on strong upward memory usage trend")
print("\nStarting monitoring...")

# Start monitoring - in real usage, this would process incoming stream data
daq.watch_out()