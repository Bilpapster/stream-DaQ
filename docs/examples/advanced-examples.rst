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

            For available field arguments and validation options, see:
            https://docs.pydantic.dev/latest/concepts/fields/
        """
        user_id: str = Field(..., min_length=1, description="User identifier")
        timestamp: int = Field(..., description="Timestamp string")
        interaction_events: float = Field(..., ge=0, description="Number of interaction events")
        temperature: Optional[float] = Field(None, ge=-50, le=100, description="Temperature reading")

    def write_to_jsonlines(data: pw.internals.Table) -> None:
        # replace the code in this function with a suitable sink operation for your use case.
        # A complete list of pathway connectors can be found here: https://pathway.com/developers/api-docs/pathway-io
        # Here, we just write the output as jsonlines to 'output.jsonlines'.
        # New quality assessment results are written (appended) to the file on the fly, when window processing is finished.
        pw.io.jsonlines.write(data, "sensor_data_output.jsonlines")

    def write_to_jsonlines_deflect(data: pw.internals.Table) -> None:
        # replace the code in this function with a suitable sink operation for your use case.
        # A complete list of pathway connectors can be found here: https://pathway.com/developers/api-docs/pathway-io
        # Here, we just write the output as jsonlines to 'output.jsonlines'.
        # New quality assessment results are written (appended) to the file on the fly, when window processing is finished.
        pw.io.jsonlines.write(data, "deflect_data_output.jsonlines")


    def example_persistent_alerts():
        """Example using persistent alert mode - always alert on schema violations."""
        print("=== Example 1: Persistent Alerts ===")

        # Create schema validator with persistent alerts
        validator = create_schema_validator(
            schema=SensorData,
            alert_mode=AlertMode.PERSISTENT,
            log_violations=False,
            raise_on_violation=False,
            deflect_violating_records=False,
            filter_respecting_records=False,
            deflection_sink=write_to_jsonlines_deflect,
            include_error_messages=True,
            column_name="schema_errors"
        )
        InputSchema = validator.create_pw_schema()

        sensor_data = pw.io.jsonlines.read(
                "data/sensor_data.jsonl",
                schema=InputSchema,
                mode="static"
            )

        # Configure StreamDaQ with schema validation
        daq = StreamDaQ().configure(
            window=Windows.tumbling(120),
            time_column="timestamp",
            wait_for_late=1,
            time_format=None,
            schema_validator=validator,
            source=sensor_data
        )

        # Add data quality measures
        daq.add(dqm.count('interaction_events'), assess="(0, 10]", name="count") \
        .add(dqm.mean('schema_errors'), assess="[0, 1]", name="mean_deflected")

        print("StreamDaQ configured with persistent schema validation")
        daq.watch_out()



    def example_first_k_alerts():
        """Example using only_on_first_k alert mode - alert only on first 3 windows."""
        print("=== Example 2: First K Windows Alerts ===")

        # Create schema validator with first-k alerts
        validator = create_schema_validator(
            schema=SensorData,
            alert_mode=AlertMode.ONLY_ON_FIRST_K,
            k_windows=3,
            log_violations=True,
            raise_on_violation=False,
            deflect_violating_records=True,
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

        # Configure StreamDaQ with schema validation
        daq = StreamDaQ().configure(
            window=Windows.tumbling(120),
            time_column="timestamp",
            wait_for_late=1,
            time_format=None,
            schema_validator=validator,
            sink_operation=write_to_jsonlines,
            source=sensor_data
        )

        # Add data quality measures
        daq.add(dqm.count('interaction_events'), assess="(0, 10]", name="count") \

        print("StreamDaQ configured with first-3-windows schema validation")
        print("Alerts will only be raised for the first 3 windows with violations")
        daq.watch_out()


    def example_conditional_alerts():
        """Example using only_if alert mode - alert only when custom condition is met."""
        print("=== Example 3: Conditional Alerts ===")

        def alert_condition(record: dict) -> bool:
            """Custom condition: alert only for high-value users or extreme temperatures."""
            user_unique = record.get("unique_users", "")

            # Alert for windows that have 2 unique users only
            two_unique = user_unique == 2

            return two_unique

        # Create schema validator with conditional alerts
        validator = create_schema_validator(
            schema=SensorData,
            alert_mode=AlertMode.ONLY_IF,
            condition_func=alert_condition,
            log_violations=False,
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

        # Configure StreamDaQ with schema validation
        daq = StreamDaQ().configure(
            window=Windows.tumbling(240),
            time_column="timestamp",
            wait_for_late=1,
            time_format=None,
            schema_validator=validator,
            source=sensor_data
        )

        # Add data quality measures
        daq.add(dqm.distinct_count('user_id'), name="unique_users")

        print("StreamDaQ configured with conditional schema validation")
        daq.watch_out()

    if __name__ == "__main__":
        """Run all examples to demonstrate different schema validation modes."""
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