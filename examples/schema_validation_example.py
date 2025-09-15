#!/usr/bin/env python3
"""
Example demonstrating Pydantic schema validation in StreamDaQ.

This example shows how to:
1. Define a Pydantic schema for your data stream
2. Configure different alert modes for schema violations
3. Integrate schema validation with StreamDaQ monitoring
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

import streamdaq
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
from streamdaq import create_schema_validator, AlertMode


# Define Pydantic schema for our data stream
class SensorData(BaseModel):
    """Pydantic model for sensor data stream validation."""
    user_id: str = Field(..., min_length=1, description="User identifier")
    timestamp: str = Field(..., description="Timestamp string")
    interaction_events: float = Field(..., ge=0, description="Number of interaction events")
    temperature: Optional[float] = Field(None, ge=-50, le=100, description="Temperature reading")
    
    class Config:
        # Allow extra fields that might be added by StreamDaQ
        extra = "allow"


def example_persistent_alerts():
    """Example using persistent alert mode - always alert on schema violations."""
    print("=== Example 1: Persistent Alerts ===")
    
    # Create schema validator with persistent alerts
    validator = create_schema_validator(
        schema=SensorData,
        alert_mode=AlertMode.PERSISTENT,
        log_violations=True,
        raise_on_violation=False  # Set to True to raise exceptions
    )
    
    # Configure StreamDaQ with schema validation
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S',
        schema_validator=validator
    )
    
    # Add data quality measures
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
       .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
       .add(dqm.median('interaction_events'), assess="[3, 8]", name="med_interact")
    
    print("StreamDaQ configured with persistent schema validation")
    print("Note: Run daq.watch_out() to start monitoring")
    print()


def example_first_k_alerts():
    """Example using only_on_first_k alert mode - alert only on first 3 windows."""
    print("=== Example 2: First K Windows Alerts ===")
    
    # Create schema validator with first-k alerts
    validator = create_schema_validator(
        schema=SensorData,
        alert_mode=AlertMode.ONLY_ON_FIRST_K,
        k_windows=3,  # Only alert on first 3 windows
        log_violations=True,
        raise_on_violation=False
    )
    
    # Configure StreamDaQ with schema validation
    daq = StreamDaQ().configure(
        window=Windows.tumbling(5),
        instance="user_id", 
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S',
        schema_validator=validator
    )
    
    # Add data quality measures  
    daq.add(dqm.count('interaction_events'), name="count") \
       .add(dqm.mean('interaction_events'), name="mean_interact")
    
    print("StreamDaQ configured with first-3-windows schema validation")
    print("Alerts will only be raised for the first 3 windows with violations")
    print()


def example_conditional_alerts():
    """Example using only_if alert mode - alert only when custom condition is met."""
    print("=== Example 3: Conditional Alerts ===")
    
    def alert_condition(record: dict) -> bool:
        """Custom condition: alert only for high-value users or extreme temperatures."""
        user_id = record.get("user_id", "")
        temperature = record.get("temperature")
        
        # Alert for VIP users or extreme temperatures
        is_vip_user = user_id.startswith("VIP_")
        extreme_temp = temperature is not None and (temperature < -20 or temperature > 80)
        
        return is_vip_user or extreme_temp
    
    # Create schema validator with conditional alerts
    validator = create_schema_validator(
        schema=SensorData,
        alert_mode=AlertMode.ONLY_IF,
        condition_func=alert_condition,
        log_violations=True,
        raise_on_violation=False
    )
    
    # Configure StreamDaQ with schema validation
    daq = StreamDaQ().configure(
        window=Windows.sliding(hop=2, duration=6),
        instance="user_id",
        time_column="timestamp", 
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S',
        schema_validator=validator
    )
    
    # Add data quality measures
    daq.add(dqm.distinct_count('user_id'), name="unique_users") \
       .add(dqm.mean('interaction_events'), name="mean_interact") \
       .add(dqm.mean('temperature'), name="mean_temp")
    
    print("StreamDaQ configured with conditional schema validation")
    print("Alerts will only be raised for VIP users or extreme temperatures")
    print()


def example_with_custom_data():
    """Example showing how schema validation works with custom data source."""
    import pathway as pw
    
    print("=== Example 4: Custom Data with Schema Validation ===")
    
    # Create sample data that includes some invalid records
    sample_data = [
        {"user_id": "user_1", "timestamp": "2024-01-01 10:00:00", "interaction_events": 5.0, "temperature": 22.5},
        {"user_id": "user_2", "timestamp": "2024-01-01 10:00:01", "interaction_events": -1.0, "temperature": 25.0},  # Invalid: negative events
        {"user_id": "", "timestamp": "2024-01-01 10:00:02", "interaction_events": 3.0, "temperature": 150.0},  # Invalid: empty user_id, extreme temp
        {"user_id": "VIP_user_3", "timestamp": "2024-01-01 10:00:03", "interaction_events": 7.0, "temperature": 85.0},  # Invalid: extreme temp, but VIP
    ]
    
    # Create pathway table from sample data
    # First convert rows to tuples format expected by table_from_rows
    rows_as_tuples = [
        (row["user_id"], row["timestamp"], row["interaction_events"], row["temperature"])
        for row in sample_data
    ]
    
    # Create schema and table
    sample_schema = pw.schema_from_types(
        user_id=str,
        timestamp=str,
        interaction_events=float,
        temperature=float
    )
    
    data_source = pw.debug.table_from_rows(
        schema=sample_schema,
        rows=rows_as_tuples
    )
    
    # Create conditional validator
    def alert_condition(record: dict) -> bool:
        user_id = record.get("user_id", "")
        return user_id.startswith("VIP_")
    
    validator = create_schema_validator(
        schema=SensorData,
        alert_mode=AlertMode.ONLY_IF,
        condition_func=alert_condition,
        log_violations=True,
        raise_on_violation=False
    )
    
    # Configure StreamDaQ
    daq = StreamDaQ().configure(
        window=Windows.tumbling(10),
        instance="user_id",
        time_column="parsed_timestamp",
        time_format='%Y-%m-%d %H:%M:%S',
        source=data_source.with_columns(
            parsed_timestamp=pw.this.timestamp.dt.strptime('%Y-%m-%d %H:%M:%S')
        ),
        schema_validator=validator
    )
    
    # Add measures including schema validation results
    daq.add(dqm.count('interaction_events'), name="total_events") \
       .add(dqm.count('_schema_valid'), name="total_records") \
       .add(dqm.count('_schema_alert'), name="total_alerts")
    
    print("StreamDaQ configured with custom data and conditional validation")
    print("Sample includes invalid records - VIP users will trigger alerts")
    
    # Get the data without starting the monitoring (for demonstration)
    result = daq.watch_out(start=False)
    print("Monitoring configured successfully")
    print()


if __name__ == "__main__":
    """Run all examples to demonstrate different schema validation modes."""
    print("StreamDaQ Schema Validation Examples")
    print("=" * 50)
    print()
    
    try:
        example_persistent_alerts()
        example_first_k_alerts()
        example_conditional_alerts()
        example_with_custom_data()
        
        print("All examples configured successfully!")
        print("\nTo actually run monitoring, call daq.watch_out() on any of the configured instances.")
        print("Schema validation will be applied before windowing in each case.")
        
    except Exception as e:
        print(f"Error running examples: {e}")
        import traceback
        traceback.print_exc()