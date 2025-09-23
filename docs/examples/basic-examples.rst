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

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

    # Configure for trend monitoring
    daq = StreamDaQ().configure(
        window=Windows.tumbling(5),  # Larger window for better trend detection
        instance="sensor_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )

    # Add trend analysis measures
    daq.add(dqm.trend('temperature'), assess=">0.2", name="heating_trend") \
        .add(dqm.trend('pressure'), assess="<-0.3", name="pressure_drop") \
        .add(dqm.trend('vibration'), assess="[-0.1, 0.1]", name="vibration_stability")

    # Start monitoring
    daq.watch_out()

**Trend Analysis Use Cases:**

- **Increasing Trends** (``assess=">0.1"``): Detect equipment heating up, growing workloads, etc.
- **Decreasing Trends** (``assess="<-0.2"``): Detect system degradation, pressure drops, etc.  
- **Stable Trends** (``assess="[-0.1, 0.1]"``): Ensure metrics remain stable within acceptable range.

The trend measure calculates the slope of a linear regression line through the data points in each window. Positive slopes indicate increasing trends, negative slopes indicate decreasing trends, and slopes near zero indicate stable behavior.