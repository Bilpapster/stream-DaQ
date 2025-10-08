🧙‍♂️ Advanced Examples
=============================

Multiple Input Sources Example
-------------------------------

Stream DaQ supports monitoring data from multiple input sources (pw.Tables) in a single instance. Each source can have its own distinct configuration (window, time_column, etc.), and the output includes source traceability via the ``_source_id`` column.

.. code-block:: python

    # pip install streamdaq
    
    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    import time
    import pathway as pw
    
    # Define two separate data sources simulating different machines/sensors
    machine1_data = [
        {"timestamp": 1, "machine_id": "m1", "temperature": 75.2, "pressure": 1200},
        {"timestamp": 2, "machine_id": "m1", "temperature": 76.1, "pressure": 1205},
        {"timestamp": 3, "machine_id": "m1", "temperature": 75.8, "pressure": 1198},
    ]
    
    machine2_data = [
        {"timestamp": 1, "machine_id": "m2", "temperature": 68.5, "pressure": 1180},
        {"timestamp": 2, "machine_id": "m2", "temperature": 69.2, "pressure": 1185},
        {"timestamp": 3, "machine_id": "m2", "temperature": 68.9, "pressure": 1182},
    ]
    
    # Create connector subjects for each machine
    class Machine1Subject(pw.io.python.ConnectorSubject):
        def run(self):
            for line in machine1_data:
                self.next(**line)
                time.sleep(0.5)
    
    class Machine2Subject(pw.io.python.ConnectorSubject):
        def run(self):
            for line in machine2_data:
                self.next(**line)
                time.sleep(0.5)
    
    class MachineSchema(pw.Schema):
        timestamp: int
        machine_id: str
        temperature: float
        pressure: int
    
    # Create separate pw.Table sources
    source1 = pw.io.python.read(Machine1Subject(), schema=MachineSchema)
    source2 = pw.io.python.read(Machine2Subject(), schema=MachineSchema)
    
    # Configure Stream DaQ with multiple sources, each with its own configuration
    daq = StreamDaQ().configure(
        sources=[
            {
                'source': source1,
                'source_id': 'machine_1',  # Identifier for traceability
                'window': Windows.sliding(hop=1, duration=3, origin=0),
                'time_column': 'timestamp',
            },
            {
                'source': source2,
                'source_id': 'machine_2',  # Identifier for traceability
                'window': Windows.sliding(hop=1, duration=2, origin=0),  # Different window!
                'time_column': 'timestamp',
            },
        ],
        window=Windows.sliding(hop=1, duration=3, origin=0),  # Default for unspecified configs
        instance="machine_id",
        time_column="timestamp",
        wait_for_late=1,
    )
    
    # Define data quality measures across all sources
    # Output will include _source_id column showing which source each record came from
    daq.add(dqm.count('machine_id'), assess=">0", name="count_readings") \
        .add(dqm.mean('temperature'), assess="[65, 80]", name="mean_temp") \
        .add(dqm.min('pressure'), assess=">1100", name="min_pressure") \
        .add(dqm.max('pressure'), assess="<1300", name="max_pressure")
    
    # Start monitoring all sources in a single instance
    daq.watch_out()

**Key Features:**

- Each source configuration is a dictionary with ``'source'`` (required) and optional parameters
- ``'source_id'`` provides traceability - defaults to ``'source_0'``, ``'source_1'``, etc.
- Each source can have its own ``'window'``, ``'time_column'``, ``'behavior'``, ``'wait_for_late'``, and ``'time_format'``
- Unspecified parameters default to the global configuration
- Output includes ``_source_id`` column to track which source generated each record
- The ``sources`` parameter cannot be used together with the ``source`` parameter

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