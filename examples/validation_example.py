from typing import Optional
from pydantic import BaseModel, Field
import pathway as pw

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
from streamdaq.SchemaValidator import SchemaValidator, create_schema_validator, AlertMode

# Define Pydantic schema for our data stream
class SensorData(BaseModel):
    """Pydantic model for sensor data stream validation."""
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
    # pw.debug.compute_and_print(data)


def example_persistent_alerts():
    """Example using persistent alert mode - always alert on schema violations."""
    print("=== Example 1: Persistent Alerts ===")

    # Create schema validator with persistent alerts
    validator = create_schema_validator(
        schema=SensorData,
        alert_mode=AlertMode.PERSISTENT,
        log_violations=True,
        raise_on_violation=False,
        deflect_violating_records=False,
        filter_respecting_records=False,
        include_error_messages=False
    )
    InputSchema = validator.create_pw_schema()

    sensor_data = pw.io.jsonlines.read(
            "../data/sensor_data.json",
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
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
       .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
       .add(dqm.median('interaction_events'), assess="[3, 8]", name="med_interact") \
       .add(dqm.sum('schema_errors'), name="count_deflected") \
       .add(dqm.mean('schema_errors'), assess="(3, 7]", name="mean_deflected")

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
        log_violations=False,
        raise_on_violation=False,
        deflect_violating_records=False,
        deflection_sink=write_to_jsonlines_deflect,
        filter_respecting_records=False,
        include_error_messages=True
    )
    InputSchema = validator.create_pw_schema()

    sensor_data = pw.io.jsonlines.read(
            "../data/sensor_data.json",
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
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
       .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
       .add(dqm.median('interaction_events'), assess="[3, 8]", name="med_interact") \
       .add(dqm.sum('schema_errors'), name="count_deflected") \
       .add(dqm.mean('schema_errors'), assess="(3, 7]", name="mean_deflected")

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
        include_error_messages=True
    )

    InputSchema = validator.create_pw_schema()

    sensor_data = pw.io.jsonlines.read(
        "../data/sensor_data.json",
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
    daq.add(dqm.distinct_count('user_id'), name="unique_users") \
       .add(dqm.mean('interaction_events'), name="mean_interact")

    print("StreamDaQ configured with conditional schema validation")
    daq.watch_out()

if __name__ == "__main__":
    """Run all examples to demonstrate different schema validation modes."""
    print("StreamDaQ Schema Validation Examples")
    print("=" * 50)
    print()

    try:
        example_persistent_alerts()
        example_first_k_alerts()
        example_conditional_alerts()

    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()