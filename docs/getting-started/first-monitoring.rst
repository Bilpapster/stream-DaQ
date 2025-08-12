🎯 First Monitoring
========================

This tutorial walks you through building a complete data quality monitoring setup from scratch. You'll learn each concept step-by-step and understand how all the pieces fit together.

What We'll Build
----------------

We'll create a monitoring system for a **real-time IoT sensor network** that tracks:

- Temperature readings from multiple sensors
- Data volume and availability issues
- Value range violations and anomalies
- Sensor health and connectivity problems

.. admonition:: What You'll Learn
   :class: tip

   - How to configure Stream DaQ for your specific use case
   - Understanding windows and their impact on monitoring
   - Creating meaningful quality assessments
   - Interpreting and acting on monitoring results

Step 1: Understanding Your Data
-------------------------------

First, let's look at the data we want to monitor:

.. code-block:: python

    import pandas as pd
    from datetime import datetime, timedelta
    import numpy as np

    # Sample IoT sensor data
    def create_sensor_data():
        """Generate realistic IoT sensor readings with some quality issues"""
        np.random.seed(42)  # For reproducible results

        sensors = ['sensor_01', 'sensor_02', 'sensor_03', 'sensor_04']
        data = []
        base_time = datetime.now()

        for i in range(100):
            for sensor in sensors:
                # Normal temperature: 18-25°C with some variation
                temp = np.random.normal(21.5, 2.0)

                # Introduce some quality issues
                if sensor == 'sensor_02' and 30 <= i <= 40:
                    # Sensor_02 gets stuck (frozen readings)
                    temp = 23.1
                elif sensor == 'sensor_03' and i > 70:
                    # Sensor_03 starts giving extreme readings
                    temp = np.random.choice([45.0, -10.0, 23.0])
                elif sensor == 'sensor_04' and 20 <= i <= 25:
                    # Sensor_04 goes offline (missing data)
                    continue

                data.append({
                    'sensor_id': sensor,
                    'temperature': round(temp, 1),
                    'timestamp': base_time + timedelta(seconds=i * 10),
                    'location': f'Building_{sensor[-1]}'
                })

        return pd.DataFrame(data)

    # Create our sample data
    sensor_data = create_sensor_data()
    print("Sample of our sensor data:")
    print(sensor_data.head(10))

Expected output:

.. code-block::

     sensor_id  temperature           timestamp    location
    0  sensor_01         24.0 2024-01-15 10:00:00  Building_1
    1  sensor_02         19.8 2024-01-15 10:00:00  Building_2
    2  sensor_03         23.2 2024-01-15 10:00:00  Building_3
    3  sensor_04         21.1 2024-01-15 10:00:00  Building_4
    ...

Step 2: Configure Your Monitor
------------------------------

Now let's set up Stream DaQ to monitor this data:

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

    # Configure the monitoring setup
    daq = StreamDaQ().configure(
        window=Windows.tumbling(60),    # 60-second windows
        instance="sensor_id",          # Monitor each sensor separately
        time_column="timestamp",       # Use timestamp for windowing
        wait_for_late=10,             # Wait 10 seconds for late arrivals
        time_format=None              # Auto-detect datetime format
    )

**Let's understand each configuration parameter:**

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **window**: ``Windows.tumbling(60)``
        :class-header: bg-info text-white

        Creates **non-overlapping 60-second windows**. Each data point belongs to exactly one window.

    .. grid-item-card:: **instance**: ``"sensor_id"``
        :class-header: bg-info text-white

        **Monitor each sensor separately**. Quality metrics are calculated per sensor per window.

    .. grid-item-card:: **time_column**: ``"timestamp"``
        :class-header: bg-info text-white

        **Which column contains the event time** for windowing and ordering.

    .. grid-item-card:: **wait_for_late**: ``10``
        :class-header: bg-info text-white

        **Wait 10 seconds** for late-arriving data before finalizing a window.

Step 3: Define Quality Measures
-------------------------------

Let's add quality checks that make sense for IoT sensor monitoring:

.. code-block:: python

    # Add data quality measures
    daq.add(
        measure=dqm.count('temperature'),
        assess=">3",  # Expect at least 4 readings per minute per sensor
        name="sufficient_data"
    ).add(
        measure=dqm.mean('temperature'),
        assess="(15.0, 30.0)",  # Average temp should be reasonable
        name="avg_temp_normal"
    ).add(
        measure=dqm.max('temperature'),
        assess="<=35.0",  # Max temp shouldn't exceed 35°C
        name="no_extreme_high"
    ).add(
        measure=dqm.min('temperature'),
        assess=">=-5.0",  # Min temp shouldn't go below -5°C
        name="no_extreme_low"
    ).add(
        measure=dqm.distinct_count('temperature'),
        assess=">1",  # Values should vary (detect frozen sensors)
        name="values_vary"
    )

**Understanding Assessment Syntax:**

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Assessment
     - Meaning
   * - ``">3"``
     - Value must be greater than 3
   * - ``"(15.0, 30.0)"``
     - Value must be between 15.0 and 30.0 (exclusive)
   * - ``"<=35.0"``
     - Value must be less than or equal to 35.0
   * - ``">=-5.0"``
     - Value must be greater than or equal to -5.0
   * - ``">1"``
     - Value must be greater than 1

Step 4: Run the Monitoring
--------------------------

Now let's start monitoring and see the results:

.. code-block:: python

    print("🚀 Starting IoT sensor monitoring...")
    print("🌡️  Analyzing temperature data quality...")

    # Run the monitoring
    results = daq.watch_out(sensor_data)

    print("✅ Monitoring complete!")
    print("\nQuality assessment results:")
    print(results)

Expected output (abbreviated):

.. code-block:: text

    🚀 Starting IoT sensor monitoring...
    🌡️  Analyzing temperature data quality...

    | sensor_id | window_start        | window_end          | sufficient_data | avg_temp_normal | no_extreme_high | no_extreme_low | values_vary |
    |-----------|---------------------|---------------------|-----------------|-----------------|-----------------|----------------|-------------|
    | sensor_01 | 2024-01-15 10:00:00 | 2024-01-15 10:01:00 | (6, True)       | (21.8, True)    | (24.5, True)    | (19.2, True)   | (6, True)   |
    | sensor_02 | 2024-01-15 10:05:00 | 2024-01-15 10:06:00 | (6, True)       | (23.1, True)    | (23.1, True)    | (23.1, True)   | (1, False)  |
    | sensor_03 | 2024-01-15 10:11:00 | 2024-01-15 10:12:00 | (6, True)       | (19.4, False)   | (45.0, False)   | (-10.0, False) | (3, True)   |
    | sensor_04 | 2024-01-15 10:03:00 | 2024-01-15 10:04:00 | (2, False)      | (21.1, True)    | (21.9, True)    | (20.3, True)   | (2, True)   |

Step 5: Analyze the Results
---------------------------

Let's examine what our monitoring detected:

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: ✅ **sensor_01**: All checks passed
        :class-header: bg-success text-white

        **Healthy sensor** - 6 readings, normal temperature range, values varying naturally

    .. grid-item-card:: ⚠️ **sensor_02**: Frozen readings detected
        :class-header: bg-warning text-dark

        **Quality issue** - Values don't vary (1 distinct value), indicating a stuck sensor

    .. grid-item-card:: 🚨 **sensor_03**: Multiple failures
        :class-header: bg-danger text-white

        **Critical issues** - Extreme temperatures detected (45°C, -10°C), average out of range

    .. grid-item-card:: 📉 **sensor_04**: Insufficient data
        :class-header: bg-info text-white

        **Connectivity issue** - Only 2 readings instead of expected 4+, sensor going offline

Understanding the Results Format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each quality check returns a **tuple**: ``(measured_value, passed_assessment)``

.. code-block:: text

    (6, True)     → Found 6 readings, passed the ">3" check ✅
    (1, False)    → Found 1 distinct value, failed the ">1" check ❌
    (45.0, False) → Max temperature 45.0°C, failed the "<=35.0" check ❌

Step 6: Create Custom Assessment Functions
------------------------------------------

Sometimes you need more sophisticated quality checks. Let's add a custom assessment function:

.. code-block:: python

    def detect_temperature_spikes(max_temp: float) -> bool:
        """
        Custom function to detect dangerous temperature spikes
        Returns False if temperature changes are too rapid
        """
        # In a real scenario, you might compare with previous windows
        # or check rate of change
        if max_temp > 40:
            print(f"⚠️ ALERT: Dangerous temperature spike detected: {max_temp}°C")
            return False
        return True

    def check_sensor_health(distinct_count: int) -> bool:
        """
        Custom function to assess overall sensor health
        """
        if distinct_count == 1:
            print(f"🔧 MAINTENANCE: Sensor appears frozen - needs attention")
            return False
        elif distinct_count < 3:
            print(f"⚠️ WARNING: Limited temperature variation detected")
            return False
        return True

    # Add custom assessments to your monitoring
    daq_advanced = StreamDaQ().configure(
        window=Windows.tumbling(60),
        instance="sensor_id",
        time_column="timestamp"
    )

    daq_advanced.add(
        measure=dqm.max('temperature'),
        assess=detect_temperature_spikes,  # Custom function
        name="spike_detection"
    ).add(
        measure=dqm.distinct_count('temperature'),
        assess=check_sensor_health,       # Another custom function
        name="sensor_health"
    )

    print("🔍 Running advanced monitoring with custom assessments...")
    advanced_results = daq_advanced.watch_out(sensor_data)

Step 7: Real-World Integration
------------------------------

In a production environment, you'd typically:

**1. Connect to Real Data Streams**

.. code-block:: python

    # Example: Reading from a message queue or database
    def connect_to_sensor_stream():
        """Connect to your real sensor data source"""
        # This could be Kafka, MQTT, database polling, etc.
        pass

**2. Set Up Alerting**

.. code-block:: python

    def send_alert(sensor_id: str, issue: str, severity: str):
        """Send alerts when quality issues are detected"""
        alert_message = f"Sensor {sensor_id}: {issue} (Severity: {severity})"

        # Send to your alerting system
        # - Email notifications
        # - Slack/Teams messages
        # - PagerDuty incidents
        # - SMS alerts
        print(f"📢 ALERT SENT: {alert_message}")

    # Custom assessment with integrated alerting
    def temperature_check_with_alerts(max_temp: float) -> bool:
        if max_temp > 35:
            send_alert("sensor_03", f"Temperature {max_temp}°C exceeds safe range", "HIGH")
            return False
        return True

**3. Store Results for Analysis**

.. code-block:: python

    def store_quality_results(results):
        """Store monitoring results for historical analysis"""
        # Save to database, data lake, or monitoring system
        # - Track quality trends over time
        # - Generate quality reports
        # - Feed into dashboards
        pass

Complete Production Example
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here's how you might structure a production monitoring setup:

.. code-block:: python

    import logging
    from datetime import datetime
    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("sensor_monitoring")

    class SensorQualityMonitor:
        def __init__(self):
            self.daq = StreamDaQ().configure(
                window=Windows.tumbling(300),  # 5-minute windows
                instance="sensor_id",
                time_column="timestamp",
                wait_for_late=30  # Wait 30 seconds for late data
            )

            self.setup_quality_checks()

        def setup_quality_checks(self):
            """Configure all quality monitoring rules"""
            self.daq.add(dqm.count('temperature'),
                        assess=">10",
                        name="sufficient_readings") \
                   .add(dqm.mean('temperature'),
                        assess="(15.0, 30.0)",
                        name="avg_temp_normal") \
                   .add(dqm.distinct_count('temperature'),
                        assess=self.check_sensor_variation,
                        name="sensor_health")

        def check_sensor_variation(self, distinct_count: int) -> bool:
            """Custom assessment for sensor health"""
            if distinct_count == 1:
                logger.warning("Frozen sensor detected!")
                return False
            return distinct_count > 3

        def monitor_stream(self, sensor_data):
            """Run quality monitoring on sensor data"""
            logger.info("Starting sensor quality monitoring...")

            try:
                results = self.daq.watch_out(sensor_data)
                self.process_results(results)
                return results

            except Exception as e:
                logger.error(f"Monitoring failed: {e}")
                raise

        def process_results(self, results):
            """Process and act on monitoring results"""
            for result in results:
                # Check for any failed assessments
                failed_checks = [name for name, (_, passed) in result.items()
                               if not passed and name != 'sensor_id']

                if failed_checks:
                    logger.warning(f"Quality issues in {result['sensor_id']}: {failed_checks}")
                    # Send alerts, update dashboards, etc.

    # Usage
    monitor = SensorQualityMonitor()
    results = monitor.monitor_stream(sensor_data)

🎉 Congratulations!
-------------------------

You've just built a comprehensive, production-ready data quality monitoring system! You now know how to:

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: ⚙️ **Configure Monitoring**
        :class-header: bg-primary text-white

        Set up windows, instances, and time handling for your specific use case

    .. grid-item-card:: 📏 **Define Quality Measures**
        :class-header: bg-success text-white

        Choose from 30+ built-in measures or create custom assessment functions

    .. grid-item-card:: 🔍 **Interpret Results**
        :class-header: bg-info text-white

        Understand what the monitoring results mean and how to act on them

    .. grid-item-card:: 🚀 **Scale to Production**
        :class-header: bg-warning text-dark

        Structure your code for real-world deployment with alerting and logging

Key Takeaways
------------------

.. admonition:: Remember These Principles
   :class: tip

   1. **Start simple** - Begin with basic measures, add complexity as needed
   2. **Think in streams** - Configure windows that match your data patterns
   3. **Custom assessments** - Use functions for complex business logic
   4. **Monitor the monitors** - Log and alert on your monitoring system itself
   5. **Iterate and improve** - Refine your quality definitions based on what you learn

What's Next?
-----------------

Now that you understand the fundamentals:

- 📚 **Explore concepts**: :doc:`../concepts/index` - Dive deeper into stream processing and quality theory
- 💡 **See more examples**: :doc:`../examples/index` - Learn from real-world use cases
- ⚙️ **Master configuration**: :doc:`../user-guide/configuration` - Unlock advanced features
- 🔧 **Browse all measures**: :doc:`../user-guide/measures` - Discover all 30+ quality measures

*Ready to make your data streams bulletproof? Stream DaQ has got you covered!* 🛡️

|made_with_love|