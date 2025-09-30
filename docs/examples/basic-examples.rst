👶 Basic Examples
===================

This section shows basic examples of using Stream DaQ for data quality monitoring.

Basic Data Quality Monitoring
------------------------------

Here's a simple example showing how to use Stream DaQ with various data quality measures:

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

    # Step 1: Configure your monitoring setup
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )

    # Step 2: Define what Data Quality means for you
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
        .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
        .add(dqm.median('interaction_events'), assess="[3, 8]", name="med_interact") \
        .add(dqm.distinct_count('interaction_events'), assess="==9", name="dist_interact")

    # Step 3: Start monitoring
    daq.watch_out()

Trend Analysis Example
----------------------

Stream DaQ also supports trend analysis to detect if your data is increasing, decreasing, or remaining stable over time:

.. code-block:: python

    # pip install streamdaq

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    import pathway as pw


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

    sensor_data = pw.debug.table_from_markdown(
    """
        | sensor_id | timestamp | temperature | vibration | speed
    1   | s_2       | 2         | 19.1        | 0.12      | 1180
    2   | s_1       | 3         | 25.3        | 0.15      | 1200
    3   | s_1       | 7         | 26.1        | 0.16      | 1205
    4   | s_2       | 7         | 28.1        | 0.12      | 1109
    5   | s_2       | 9         | 22.8        | 0.13      | 1185
    6   | s_1       | 12        | 27.2        | 0.22      | 1210
    7   | s_2       | 15        | 19.4        | 0.17      | 1190
    8   | s_1       | 18        | 28.5        | 0.21      | 1215
    9   | s_2       | 22        | 24.1        | 0.16      | 1195
    10  | s_1       | 25        | 29.8        | 0.16      | 1220
    11  | s_2       | 29        | 24.9        | 0.15      | 1200
    """
    )

    # Step 1: Configure Stream DaQ for trend analysis monitoring
    daq = StreamDaQ().configure(
        source=sensor_data,
        window=Windows.tumbling(15),
        instance="sensor_id",
        time_column="timestamp",
        wait_for_late=1
    )

    # Step 2: Define trend-based data quality measures
    daq.add(dqm.trend('temperature', 'timestamp'), assess=">0.2", name="heating_trend") \
        .add(dqm.trend('vibration', 'timestamp'), assess=is_stable_trend, name="vibration_stability") \
        .add(dqm.trend('speed', 'timestamp'), assess=is_strong_trend, name="speed_variation") \
        .add(dqm.min('temperature'), assess=">20", name="min_temp") \
        .add(dqm.max('temperature'), assess="<80", name="max_temp") \
        .add(dqm.range_conformance_fraction('temperature', 20, 80), assess=">0.8", name="temp_in_range")

    # Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py

    # Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
    daq.watch_out()

**Trend Analysis Use Cases:**

- **Increasing Trends** (``assess=">0.1"``): Detect equipment heating up, growing workloads, etc.
- **Decreasing Trends** (``assess="< -0.2"``): Detect system degradation, pressure drops, etc.  
- **Stable Trends** (``assess="[-0.1, 0.1]"``): Ensure metrics remain stable within acceptable range.

The trend measure calculates the slope of a linear regression line through the data points in each window. Positive slopes indicate increasing trends, negative slopes indicate decreasing trends, and slopes near zero indicate stable behavior.

Trend analysis complements traditional min-max and range checks for comprehensive data quality monitoring. While threshold checks validate current values, trend analysis ensures data consistency over time by detecting unexpected patterns or gradual shifts that could indicate sensor drift or measurement errors.

Luckily, Stream DaQ offers a suite of over 30 data quality measures, including range conformance, profiling statistics, trend analysis and many more - making comprehensive data quality monitoring both powerful and effortless!
