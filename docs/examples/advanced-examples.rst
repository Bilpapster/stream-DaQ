🧙‍♂️ Advanced Examples
=============================

This section demonstrates Stream DaQ's advanced capabilities for complex monitoring scenarios. You'll learn how to leverage multi-source task architecture, handle compact IoT data formats, and implement sophisticated schema validation patterns that go beyond basic quality monitoring.

Multi-Source Task Monitoring
-----------------------------

Stream DaQ's task-based architecture enables monitoring multiple independent data sources within a single StreamDaQ instance. This powerful approach eliminates the need to manage multiple monitoring processes while providing complete isolation and flexibility for each data source.

**Key Benefits:**
- **Unified Management**: One StreamDaQ instance orchestrates multiple monitoring tasks
- **Independent Configuration**: Each task has its own windowing, schema validation, and quality checks
- **Error Isolation**: Non-critical task failures don't affect other tasks
- **Resource Efficiency**: Shared infrastructure with coordinated execution

**Real-World Scenario: Smart City Platform**

.. code-block:: python

    # pip install streamdaq
    
    import pathway as pw
    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows, CompactData
    from streamdaq.SchemaValidator import create_schema_validator, AlertMode
    from pydantic import BaseModel, Field
    
    # Create StreamDaQ instance for multi-source monitoring
    daq = StreamDaQ()
    
    # Task 1: IoT Environmental Sensors (Critical - affects public safety)
    iot_task = daq.new_task("environmental_sensors", critical=True)
    iot_task.configure(
        source=iot_sensor_data,
        window=Windows.sliding(duration=300, hop=60),  # 5-min windows, updated every minute
        time_column="sensor_timestamp",
        instance="sensor_id",
        compact_data=CompactData()  # Handle compact sensor data automatically
    )
    
    # Environmental quality checks
    iot_task.check(dqm.count('temperature'), must_be=">50", name="temp_readings") \
            .check(dqm.missing_count('air_quality'), must_be="<5", name="air_quality_missing") \
            .check(dqm.mean('temperature'), must_be="(15, 35)", name="temp_range")
    
    # Task 2: User Engagement Analytics (Non-critical - for business insights)
    user_task = daq.new_task("user_analytics", critical=False)
    user_task.configure(
        source=user_events_data,
        window=Windows.tumbling(duration=3600),  # Hourly analysis
        time_column="event_time",
        instance="user_id"
    )
    
    # User engagement quality checks
    user_task.check(dqm.distinct_count('action'), must_be=">3", name="action_diversity") \
             .check(dqm.mean('session_duration'), must_be="(30, 600)", name="session_quality")
    
    # Task 3: Financial Transactions (Critical - affects payments)
    finance_task = daq.new_task("financial_monitoring", critical=True)
    finance_task.configure(
        source=transaction_data,
        window=Windows.tumbling(duration=60),  # Real-time fraud detection
        time_column="transaction_time",
        wait_for_late=0,  # No tolerance for late financial data
        schema_validator=transaction_validator  # Strict validation
    )
    
    # Financial compliance checks
    finance_task.check(dqm.count('amount'), must_be=">0", name="transaction_volume") \
               .check(dqm.sum('amount'), must_be="(1000, 100000)", name="total_amount")
    
    # Start monitoring all tasks concurrently
    daq.watch_out()

**Task Independence Features:**

- **Different Data Formats**: IoT sensors use compact data, others use native format
- **Different Windowing**: Sliding windows for real-time sensors, tumbling for batch analytics
- **Different Criticality**: Environmental and financial monitoring are critical, analytics are not
- **Independent Error Handling**: Non-critical failures don't stop critical monitoring

**Backward Compatibility**

The new task-based architecture maintains full compatibility with existing Stream DaQ code:

.. code-block:: python

    # Existing code continues to work unchanged
    daq = StreamDaQ().configure(
        source=legacy_data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )
    daq.add(dqm.count('events'), assess=">10", name="count")  # Still works (with deprecation warning)
    daq.watch_out()
    
    # Mixed usage: combine old and new approaches
    daq = StreamDaQ()
    
    # Keep existing configuration (creates default task internally)
    daq.configure(source=legacy_data, window=Windows.tumbling(60), time_column="timestamp")
    daq.check(dqm.count('events'), must_be=">10", name="legacy_count")
    
    # Add new tasks using new API
    new_task = daq.new_task("additional_monitoring")
    new_task.configure(source=new_data, window=Windows.sliding(120, 30), time_column="timestamp")
    new_task.check(dqm.mean('values'), must_be="(0, 100)", name="avg_check")
    
    daq.watch_out()  # Monitors both legacy and new tasks

**Error Handling and Recovery**

.. code-block:: python

    try:
        daq.watch_out()
    except CriticalTaskFailureError as e:
        print(f"Critical task '{e.task_name}' failed: {e.original_error}")
        print("All monitoring stopped for safety - implement recovery procedures")
    except Exception as e:
        print(f"Non-critical task failure: {e}")
        print("Other tasks continue running normally")

**When to Use Multi-Source Tasks:**

- **IoT Platforms**: Monitor sensors, user devices, and system infrastructure simultaneously
- **E-commerce**: Track payments (critical), user behavior (non-critical), and inventory (critical)
- **Financial Services**: Monitor transactions, market data, and compliance reporting
- **Smart Cities**: Environmental sensors, traffic monitoring, and public safety systems
- **Manufacturing**: Production lines, quality control, and predictive maintenance

For complete examples, see:
- ``examples/multi_source_monitoring.py`` - Comprehensive multi-source scenario
- ``examples/mixed_api_usage.py`` - Backward compatibility demonstration  
- ``examples/critical_task_handling.py`` - Error handling and recovery patterns

Compact Data Monitoring Example
--------------------------------

Stream DaQ provides seamless support for compact data formats commonly used in IoT and resource-constrained environments. Instead of manually transforming compact data into individual records, Stream DaQ handles this automatically, allowing you to focus on defining meaningful quality measures.

.. seealso::
   
   For conceptual background on compact vs native data formats, see :doc:`../concepts/compact-vs-native-data`.

**What makes data "compact"?**

Compact data represents multiple field values in a single record, typically using arrays or lists. This format is prevalent in IoT scenarios because it:

- **Reduces bandwidth usage** by ~60% compared to individual field transmissions
- **Minimizes storage requirements** on resource-constrained devices  
- **Enables efficient batch transmission** of multiple sensor readings
- **Optimizes network protocols** for wireless sensor networks

**Common IoT scenarios using compact data:**

- Environmental monitoring stations (temperature, humidity, pressure)
- Industrial sensor networks (vibration, temperature, speed)
- Smart building systems (occupancy, air quality, energy usage)
- Vehicle telemetry (GPS coordinates, speed, fuel consumption, engine metrics)

.. code-block:: python

    # pip install streamdaq
    
    import pathway as pw
    from streamdaq import DaQMeasures as dqm
    from streamdaq import CompactData, Windows, StreamDaQ

    # Configuration for compact IoT sensor data
    FIELDS_COLUMN = "fields"
    FIELDS = ["temperature", "humidity", "pressure"]  # IoT sensor measurements
    VALUES_COLUMN = "values"
    TIMESTAMP_COLUMN = "timestamp"

    # Example compact data source (simulating IoT sensor network)
    class CompactDataSource(pw.io.python.ConnectorSubject):
        """Simulates IoT sensors sending compact data format."""
        def run(self):
            nof_fields = len(FIELDS)
            nof_compact_rows = 5
            timestamp = value = 0
            for _ in range(nof_compact_rows):
                message = {
                    TIMESTAMP_COLUMN: timestamp,
                    FIELDS_COLUMN: FIELDS,
                    VALUES_COLUMN: [value + i for i in range(nof_fields)]
                }
                value += len(FIELDS)
                timestamp += 1
                self.next(**message)

    # Define schema for compact data structure
    schema_dict = {
        TIMESTAMP_COLUMN: int,
        FIELDS_COLUMN: list[str],
        VALUES_COLUMN: list[int | None]  # Supports missing values
    }
    schema = pw.schema_from_dict(schema_dict)

    # Create compact data stream
    compact_data_stream = pw.io.python.read(
        CompactDataSource(),
        schema=schema,
    )

    # Configure Stream DaQ for automatic compact data handling
    daq = StreamDaQ().configure(
        window=Windows.sliding(duration=3, hop=1, origin=0),
        source=compact_data_stream,
        time_column=TIMESTAMP_COLUMN,
        wait_for_late=1,  # Handle late IoT data arrivals
        
        # Stream DaQ automatically transforms compact to native format
        compact_data=CompactData() \
            .with_fields_column(FIELDS_COLUMN) \
            .with_values_column(VALUES_COLUMN) \
            .with_values_dtype(int)
    )

    # Define quality measures for individual sensor fields
    # Notice: Direct field access despite compact input format!
    daq.add(dqm.count('pressure'), name="readings") \
       .add(dqm.missing_count('temperature') + 
            dqm.missing_count('pressure') + 
            dqm.missing_count('humidity'),
            assess="<2", name="missing_readings") \
       .add(dqm.is_frozen('humidity'), name="frozen_humidity_sensor")

    # Start monitoring
    daq.watch_out()

**Stream DaQ's Automatic Transformation Benefits:**

1. **No Manual Preprocessing**: Stream DaQ internally converts compact data to native format for quality analysis
2. **Seamless Field Access**: Reference individual fields (``temperature``, ``humidity``, ``pressure``) directly in quality measures
3. **Missing Value Handling**: Automatic support for ``None`` values common in real-world IoT scenarios  
4. **Type Safety**: Configurable data type handling with validation
5. **Temporal Alignment**: Proper time-based windowing despite compact input format

**Compact vs Native Data Comparison:**

.. code-block:: json

    // Compact format (1 record):
    {
        "timestamp": 1,
        "fields": ["temperature", "humidity", "pressure"], 
        "values": [23.5, 65.2, 1013.25]
    }

    // Equivalent native format (3 records):
    {"timestamp": 1, "temperature": 23.5}
    {"timestamp": 1, "humidity": 65.2} 
    {"timestamp": 1, "pressure": 1013.25}

**Why This Matters for IoT:**

Without Stream DaQ's automatic handling, you would typically need to:

- Manually unpack compact rows into individual field records
- Handle missing values and data type conversions
- Manage temporal alignment across different fields
- Write custom transformation logic before quality monitoring

Stream DaQ eliminates this preprocessing pipeline, allowing you to focus on defining meaningful quality measures rather than data transformation logic. This is especially valuable in resource-constrained environments where development time and computational efficiency are critical.

For a complete working example with detailed comments, see the ``examples/compact_data.py`` file in the examples directory. To understand the conceptual differences between compact and native data formats, see :doc:`../concepts/compact-vs-native-data`.

Schema Validation Example
--------------------------

Stream DaQ provides comprehensive schema validation capabilities through Pydantic models, enabling automatic data quality enforcement with flexible alert strategies. The validation system can detect type mismatches, constraint violations, and missing required fields while offering sophisticated control over when and how violations are reported.

**Key Validation Features:**

- **Type Safety**: Automatic validation of data types and constraints using Pydantic models
- **Flexible Alert Modes**: Control when validation alerts are triggered (persistent, first-k windows, or conditional)
- **Error Handling**: Configure whether to log, raise exceptions, or deflect invalid records
- **Custom Conditions**: Define business logic for when validation alerts should fire
- **Integration**: Seamlessly works with Stream DaQ's quality monitoring pipeline

.. note::

   Schema validation acts as the first line of defense in data quality monitoring. It ensures data conforms to expected structure and constraints before quality measures are computed, preventing downstream errors and providing early warning of data pipeline issues.

.. code-block:: python

    # pip install streamdaq

    from typing import Optional
    from pathway import io
    from pydantic import BaseModel, Field
    import pathway as pw

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    from streamdaq.SchemaValidator import create_schema_validator, AlertMode

    class SensorData(BaseModel):
        """
        Pydantic model for sensor data stream validation.

        This model defines the expected structure and constraints for IoT sensor data.
        Each field includes validation rules that will be enforced on incoming data.

        For available field arguments and validation options, see:
        https://docs.pydantic.dev/latest/concepts/fields/
        """
        user_id: str = Field(..., min_length=1, description="User identifier")
        timestamp: int = Field(..., description="Timestamp string")
        interaction_events: float = Field(..., ge=0, description="Number of interaction events")
        temperature: Optional[float] = Field(None, ge=-50, le=100, description="Temperature reading")

    def write_to_jsonlines(data: pw.internals.Table) -> None:
        """
        Output sink for valid data records.
        Replace with your preferred data destination (database, message queue, etc.).
        """
        pw.io.jsonlines.write(data, "sensor_data_output.jsonlines")

    def write_to_jsonlines_deflect(data: pw.internals.Table) -> None:
        """
        Output sink for invalid/deflected data records.
        Useful for debugging and data quality investigation.
        """
        pw.io.jsonlines.write(data, "deflect_data_output.jsonlines")

    def example_persistent_alerts():
        """
        Example 1: Persistent Alert Mode

        Persistent alerts ensure that schema violations are always reported,
        regardless of when they occur. This is the most strict validation mode,
        suitable for critical data streams where every violation must be addressed.

        Use case: Financial transaction data where regulatory compliance requires
        complete data validation audit trails.
        """
        print("=== Example 1: Persistent Alerts ===")

        # Create schema validator with persistent alerts
        validator = create_schema_validator(
            schema=SensorData,
            alert_mode=AlertMode.PERSISTENT,      # Always alert on violations
            log_violations=False,                 # Don't log to console (reduce noise)
            raise_on_violation=False,             # Continue processing despite violations
            deflect_violating_records=False,      # Keep invalid records in main stream
            filter_respecting_records=False,      # Include valid records in output
            deflection_sink=write_to_jsonlines_deflect,
            include_error_messages=True,          # Add error details to records
            column_name="schema_errors"           # Column name for error information
        )
        InputSchema = validator.create_pw_schema()

        # Load sensor data for validation
        sensor_data = pw.io.jsonlines.read(
                "data/sensor_data.jsonl",
                schema=InputSchema,
                mode="static"
            )

        # Configure StreamDaQ with strict schema validation
        daq = StreamDaQ().configure(
            window=Windows.tumbling(120),          # 2-minute analysis windows
            time_column="timestamp",
            wait_for_late=1,                      # 1-second grace period
            time_format=None,                     # Use raw timestamp values
            schema_validator=validator,           # Apply validation rules
            source=sensor_data
        )

        # Add quality measures that work with validated data
        daq.add(dqm.count('interaction_events'), assess="(0, 10]", name="count") \
           .add(dqm.mean('schema_errors'), assess="[0, 1]", name="mean_deflected")

        print("StreamDaQ configured with persistent schema validation")
        daq.watch_out()

    def example_first_k_alerts():
        """
        Example 2: First-K Windows Alert Mode

        This mode only alerts during the first K windows with violations,
        then suppresses further alerts. Useful during system startup or
        after configuration changes when some violations are expected.

        Use case: IoT sensor deployment where initial data quality issues
        are common but should stabilize after calibration period.
        """
        print("=== Example 2: First K Windows Alerts ===")

        # Create schema validator with limited alert window
        validator = create_schema_validator(
            schema=SensorData,
            alert_mode=AlertMode.ONLY_ON_FIRST_K,
            k_windows=3,                          # Only alert for first 3 violation windows
            log_violations=True,                  # Log violations during alert period
            raise_on_violation=False,
            deflect_violating_records=True,       # Separate invalid records for analysis
            deflection_sink=write_to_jsonlines_deflect,
            filter_respecting_records=False,
            include_error_messages=False
        )
        InputSchema = validator.create_pw_schema()

        sensor_data = pw.io.jsonlines.read(
                "data/sensor_data.jsonl",
                schema=InputSchema,
                mode="static"
            )

        # Configure monitoring with grace period for violations
        daq = StreamDaQ().configure(
            window=Windows.tumbling(120),
            time_column="timestamp",
            wait_for_late=1,
            time_format=None,
            schema_validator=validator,
            sink_operation=write_to_jsonlines,
            source=sensor_data
        )

        # Focus on core quality metrics during stabilization
        daq.add(dqm.count('interaction_events'), assess="(0, 10]", name="count")

        print("StreamDaQ configured with first-3-windows schema validation")
        print("Alerts will only be raised for the first 3 windows with violations")
        daq.watch_out()

    def example_conditional_alerts():
        """
        Example 3: Conditional Alert Mode

        Conditional alerts provide fine-grained control over when validation
        violations should trigger alerts. This allows business logic to determine
        the criticality of violations based on data content or operational context.

        Use case: E-commerce platform where validation strictness varies based
        on customer tier, transaction value, or system load conditions.
        """
        print("=== Example 3: Conditional Alerts ===")

        def alert_condition(record: dict) -> bool:
            """
            Custom business logic for alert triggering.

            This example alerts only when there are exactly 2 unique users
            in a window, indicating a specific operational scenario where
            data quality is more critical.

            Args:
                record: Window-level aggregated data including quality measures

            Returns:
                bool: True if alert should be triggered for this window
            """
            user_unique = record.get("unique_users", "")

            # Alert for windows that have exactly 2 unique users
            # (This could represent a critical operational state)
            two_unique = user_unique == 2

            return two_unique

        # Create schema validator with custom alerting logic
        validator = create_schema_validator(
            schema=SensorData,
            alert_mode=AlertMode.ONLY_IF,
            condition_func=alert_condition,       # Custom condition function
            log_violations=False,                 # Suppress general logging
            raise_on_violation=False,
            deflect_violating_records=False,
            deflection_sink=write_to_jsonlines_deflect,
            filter_respecting_records=False,
            include_error_messages=False
        )

        InputSchema = validator.create_pw_schema()

        sensor_data = pw.io.jsonlines.read(
            "data/sensor_data.jsonl",
            schema=InputSchema,
            mode="static"
        )

        # Configure monitoring with business-aware validation
        daq = StreamDaQ().configure(
            window=Windows.tumbling(240),          # Longer windows for conditional logic
            time_column="timestamp",
            wait_for_late=1,
            time_format=None,
            schema_validator=validator,
            source=sensor_data
        )

        # Add measure that feeds into conditional logic
        daq.add(dqm.distinct_count('user_id'), name="unique_users")

        print("StreamDaQ configured with conditional schema validation")
        daq.watch_out()

    if __name__ == "__main__":
        """
        Run all examples to demonstrate different schema validation strategies.

        This demonstrates the progression from strict validation (persistent)
        to graceful startup handling (first-k) to business-aware validation (conditional).
        """
        print("StreamDaQ Schema Validation Examples")
        print("=" * 50)
        print()

        try:
            example_persistent_alerts()
            print()
            example_first_k_alerts()
            print()
            example_conditional_alerts()

        except Exception as e:
            print(f"Error running examples: {e}")
            import traceback
            traceback.print_exc()

**Schema Validation Strategies:**

.. list-table::
   :widths: 20 30 50
   :header-rows: 1

   * - Alert Mode
     - Use Case
     - Description
   * - ``PERSISTENT``
     - Critical systems, compliance
     - Always alert on violations, maintain complete audit trail
   * - ``ONLY_ON_FIRST_K``
     - System startup, testing
     - Alert only during initial stabilization period
   * - ``ONLY_IF``
     - Business-aware validation
     - Alert based on custom business logic and operational context

**Configuration Best Practices:**

- **Critical Systems**: Use ``PERSISTENT`` mode with ``raise_on_violation=True`` for immediate failure on invalid data
- **Development/Testing**: Use ``ONLY_ON_FIRST_K`` mode to handle expected initial data quality issues
- **Production Systems**: Use ``ONLY_IF`` mode with business logic to balance data quality monitoring with operational stability
- **Debugging**: Enable ``deflect_violating_records=True`` and ``include_error_messages=True`` to analyze validation failures

For complete examples with sample data files, see the ``examples/schema_validation.py`` file in the examples directory.


Anomaly Detection Example
-------------------------

Stream DaQ provides sophisticated anomaly detection capabilities through its integrated statistical detector. This module enables automatic identification of outliers and abnormal patterns in streaming data without requiring manual threshold configuration.

**Key Features:**

- **Statistical Baseline Learning**: Automatically establishes normal operating ranges from historical data
- **Z-score Based Detection**: Uses configurable standard deviation thresholds for anomaly identification
- **Multi-Column Analysis**: Monitors multiple sensor dimensions simultaneously
- **Adaptive Thresholding**: Adapts to changing data patterns over time
- **Top-K Reporting**: Reports only the features with highest anomaly scores per window, focusing attention on the most significant deviations from baseline statistics.
.. note::

   Statistical anomaly detection complements rule-based monitoring by identifying subtle patterns and deviations that static thresholds might miss. This approach is particularly effective for detecting sensor drift, equipment degradation, or unexpected operational changes.

**Real-World Scenario: Industrial Sensor Monitoring**

.. code-block:: python

    # pip install streamdaq

    import random
    import time
    import pathway as pw
    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    from streamdaq.anomaly_detectors.StatisticalDetector import StatisticalDetector

    class InputSchema(pw.Schema):
        """Schema for industrial sensor data streams."""
        colA: int  # Primary sensor (e.g., temperature, pressure)
        colB: int  # Secondary sensor (e.g., humidity, vibration)
        timestamp: int  # Unix timestamp

    class SensorDataSource(pw.io.python.ConnectorSubject):
        """Simulates industrial sensors with periodic anomalies."""

        def run(self):
            # Define specific timestamps where anomalies occur
            outlier_timestamps = {28, 32, 53, 56, 62, 78, 81, 98}

            for timestamp in range(1, 100):
                if timestamp in outlier_timestamps:
                    # Generate anomalous values (equipment malfunction simulation)
                    colA_value = random.choice([2000, 2500, 3000, -5000, -2000])
                else:
                    # Normal operating range values
                    colA_value = random.randint(0, 50)

                message = {
                    "timestamp": timestamp,
                    "colA": colA_value,
                    "colB": random.randint(0, 50)
                }
                time.sleep(0.2)  # Simulate real-time delay
                self.next(**message)

    # Create streaming data source
    sensor_data = pw.io.python.read(SensorDataSource(), schema=InputSchema)

    # Step 1: Initialize Stream DaQ for anomaly detection
    daq = StreamDaQ()

    # Step 2: Create a critical monitoring task
    sensor_task = daq.new_task("sensor_anomaly_detection", critical=True)

    # Step 3: Configure statistical measures for baseline computation
    measures = [("min", "colA"), ("max", "colB"), ("mean", "colA")]

    # Step 4: Configure the Statistical Anomaly Detector
    detector = StatisticalDetector(
        buffer_size=10,    # Historical windows for baseline
        warmup_time=2,     # Initial windows before detection starts
        threshold=1.5,     # Z-score threshold (1.5 = moderate sensitivity)
        top_k=2,           # Report top 2 anomalies per window
        measures=measures  # Statistical measures to monitor
    )

    # Step 5: Configure the monitoring task
    sensor_task.configure(
        source=sensor_data,
        window=Windows.tumbling(5),  # 5-unit tumbling windows
        time_column="timestamp",
        wait_for_late=1,              # Grace period for late data
        detector=detector             # Anomaly detection engine
    )

    # Step 6: Start real-time anomaly monitoring
    daq.watch_out()

**Anomaly Detection Configuration Options:**

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Parameter
     - Default
     - Description
   * - ``buffer_size``
     - 10
     - Number of historical windows for baseline statistics
   * - ``warmup_time``
     - 2
     - Windows processed before anomaly detection starts
   * - ``threshold``
     - 2.0
     - Z-score threshold for anomaly detection (1.5=sensitive, 3.0=conservative)
   * - ``top_k``
     - 5
     - Number of top features with highest anomaly scores to report per window. Focuses attention on the most significant deviations from baseline statistics, reducing noise from minor anomalies.

**Detection Scenarios:**

1. **Multi-Column Anomaly Detection**:

   .. code-block:: python

       # Monitor multiple sensors simultaneously
       measures = [("mean", "temperature"), ("std", "pressure"), ("max", "vibration")]

       # Alternative: Apply measures to all columns
       measures = ["min", "max", "mean", "std"]
       columns = ["temperature", "pressure", "vibration"]

2. **Adaptive Threshold Tuning**:

   .. code-block:: python

       # Conservative detection (fewer false positives)
       detector = StatisticalDetector(threshold=3.0)

       # Sensitive detection (catch subtle anomalies)
       detector = StatisticalDetector(threshold=1.5)

**Use Cases for Statistical Anomaly Detection:**

- **Industrial IoT**: Equipment monitoring for predictive maintenance
- **Financial Services**: Transaction fraud detection and market anomaly identification
- **Smart Cities**: Environmental sensor monitoring for air quality and traffic patterns
- **Healthcare**: Patient vital signs monitoring and medical device performance
- **Energy Management**: Power consumption anomaly detection and grid stability monitoring

.. tip::

   Statistical anomaly detection works best when combined with domain-specific rule-based monitoring. Use statistical detection to identify unexpected patterns and rule-based checks for known failure conditions.

For a complete working example with detailed sensor simulation and anomaly injection, see the ``examples/anomaly_detection.py`` file in the examples directory.