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

.. code-block::

    🚀 Starting IoT sensor monitoring...
    🌡️  Analyzing temperature data quality...

    | sensor_id | window_start        | window_end          | sufficient_data | avg_temp_normal | no_extreme_high | no_extreme_low | values_vary |
    |-----------|---------------------|---------------------|-----------------|-----------------|-----------------|----------------|-------------|
    | sensor_01 | 2024-01-15 10:00:00 | 2024-01-15 10:01:00 | (6, True)       | (21.8, True)    | (24.5, True)    | (19.2, True)   | (6, True)   |
    | sensor_02 | 2024-01-15 10:05:00 | 2024-01-15 10:06:00 | (6, True)       | (23.1, True)    | (23.1, True)    | (23.1, True)   | (1, False)  |
    | sensor_03 | 2024-01-15 10:11:00 | 2024-01-15 10:12:00 | (6, True)       | (19.4, False)   | (45.0, False)   | (-10.0, False) | (3, True)   |
    | sensor_04 | 2024-01-15 10:03:00 | 2024-01-15 10:04:00 | (2, False)      | (21