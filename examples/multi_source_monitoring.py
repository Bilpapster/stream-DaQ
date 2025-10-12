# pip install streamdaq

"""
Multi-Source Data Quality Monitoring Example

This example demonstrates Stream DaQ's new task-based architecture for monitoring
multiple independent data sources with different configurations, windowing strategies,
and quality checks.

Scenario: A smart city platform monitoring three different data streams:
1. IoT sensors (compact data format) - Environmental monitoring
2. User events (native format) - Application usage analytics  
3. Financial transactions (native format) - Payment processing
"""

import pathway as pw
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows, CompactData
from streamdaq.SchemaValidator import create_schema_validator, AlertMode
from pydantic import BaseModel, Field
from typing import Optional


# Step 1: Define data sources and schemas

# IoT Sensor Data Source (Compact Format)
class IoTSensorSource(pw.io.python.ConnectorSubject):
    """Simulates IoT sensors sending compact environmental data."""
    
    def run(self):
        fields = ["temperature", "humidity", "pressure", "air_quality"]
        for timestamp in range(1, 11):  # 10 readings
            # Simulate some missing readings for quality testing
            values = [
                20 + (timestamp % 10),  # temperature
                45 + (timestamp % 20),  # humidity  
                1013 + (timestamp % 5), # pressure
                None if timestamp % 7 == 0 else 50 + (timestamp % 30)  # air_quality (some missing)
            ]
            
            message = {
                "timestamp": timestamp,
                "sensor_id": f"sensor_{(timestamp % 3) + 1}",  # 3 different sensors
                "fields": fields,
                "values": values
            }
            self.next(**message)

# User Events Data Source (Native Format)
class UserEventsSource(pw.io.python.ConnectorSubject):
    """Simulates user interaction events from a web application."""
    
    def run(self):
        actions = ["login", "view_page", "click_button", "logout", "purchase"]
        for timestamp in range(1, 16):  # 15 events
            message = {
                "timestamp": timestamp,
                "user_id": f"user_{(timestamp % 5) + 1}",  # 5 different users
                "action": actions[timestamp % len(actions)],
                "session_duration": 60 + (timestamp % 300),  # 1-6 minutes
                "page_views": 1 + (timestamp % 10)
            }
            self.next(**message)

# Financial Transactions Data Source (Native Format) 
class TransactionSource(pw.io.python.ConnectorSubject):
    """Simulates financial transaction data with strict validation requirements."""
    
    def run(self):
        from numpy.random import choice
        
        for timestamp in range(1, 16):  # 15 transactions
            # Simulate some invalid amounts for testing
            amount = 100.0 + (timestamp * 50.0)
            if timestamp % 8 == 0:  # Occasionally invalid amount
                amount = -50.0  # Invalid negative amount
                
            message = {
                "timestamp": timestamp,
                "transaction_id": f"tx_{timestamp:04d}",
                "user_id": f"user_{(timestamp % 3) + 1}",
                "amount": amount,
                "currency": choice(["USD", "EUR", "GBP"], p=[0.5, 0.2, 0.3]),
                "merchant": f"merchant_{(timestamp % 4) + 1}"
            }
            self.next(**message)


# Step 2: Define schemas for validation

class TransactionSchema(BaseModel):
    """Pydantic schema for financial transaction validation."""
    timestamp: int
    transaction_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0, description="Amount must be positive")
    currency: str = Field(..., pattern="^[A-Z]{3}$", description="3-letter currency code")
    merchant: str = Field(..., min_length=1)


# Step 3: Create data sources with proper schemas

# IoT sensors (compact data)
iot_schema = pw.schema_from_dict({
    "timestamp": int,
    "sensor_id": str,
    "fields": list[str],
    "values": list[float | None]
})

iot_data = pw.io.python.read(IoTSensorSource(), schema=iot_schema)

# User events (native data)
user_events_schema = pw.schema_from_dict({
    "timestamp": int,
    "user_id": str,
    "action": str,
    "session_duration": int,
    "page_views": int
})

user_events_data = pw.io.python.read(UserEventsSource(), schema=user_events_schema)

# Financial transactions (native data with validation)
transaction_validator = create_schema_validator(
    schema=TransactionSchema,
    alert_mode=AlertMode.PERSISTENT,
    log_violations=True,
    raise_on_violation=False,
    deflect_violating_records=True,
    include_error_messages=False
)

transaction_schema = transaction_validator.create_pw_schema()
transaction_data = pw.io.python.read(TransactionSource(), schema=transaction_schema)


# Step 4: Configure multi-source monitoring with Stream DaQ

print("=== Multi-Source Data Quality Monitoring ===")
print("Monitoring 3 independent data streams with different configurations:\n")

# Create StreamDaQ instance
daq = StreamDaQ()

# Task 1: IoT Environmental Sensors (Critical - affects safety systems)
print("Configuring Task 1: IoT Environmental Sensors (CRITICAL)")
iot_task = daq.new_task("iot_sensors", critical=True)
iot_task.configure(
    source=iot_data,
    window=Windows.sliding(duration=5, hop=2),  # 5-second sliding windows, updated every 2 seconds
    time_column="timestamp",
    instance="sensor_id",  # Monitor each sensor separately
    wait_for_late=1,
    compact_data=CompactData()  # Handle compact data format automatically
)

# Add environmental quality checks
iot_task.check(dqm.count('temperature'), must_be=">2", name="temp_readings_count") \
        .check(dqm.missing_count('air_quality'), must_be="<2", name="air_quality_missing") \
        .check(dqm.mean('temperature'), must_be="(15, 35)", name="temp_range_check") \
        .check(dqm.max('humidity'), must_be="<80", name="humidity_max_check")

print("✓ IoT sensors task configured with compact data handling and sliding windows")

# Task 2: User Engagement Analytics (Non-critical - for business insights)
print("Configuring Task 2: User Engagement Analytics (NON-CRITICAL)")
user_task = daq.new_task("user_analytics", critical=False)
user_task.configure(
    source=user_events_data,
    window=Windows.tumbling(duration=10),  # 10-second tumbling windows
    time_column="timestamp",
    instance="user_id",  # Monitor per user
    wait_for_late=2
)

# Add user engagement quality checks
user_task.check(dqm.distinct_count('action'), must_be=">2", name="action_diversity") \
         .check(dqm.mean('session_duration'), must_be="(30, 600)", name="session_duration_check") \
         .check(dqm.count('page_views'), must_be=">5", name="total_page_views")

print("✓ User analytics task configured with tumbling windows and engagement metrics")

# Task 3: Financial Transaction Monitoring (Critical - affects payments)
print("Configuring Task 3: Financial Transactions (CRITICAL)")
finance_task = daq.new_task("financial_transactions", critical=True)
finance_task.configure(
    source=transaction_data,
    window=Windows.tumbling(duration=5),  # 5-second tumbling windows for real-time fraud detection
    time_column="timestamp",
    wait_for_late=0,  # No tolerance for late financial data
    schema_validator=transaction_validator  # Strict schema validation
)

# Add financial quality and compliance checks
finance_task.check(dqm.count('amount'), must_be=">0", name="transaction_volume") \
            .check(dqm.set_conformance_fraction('currency', allowed_values={"EUR", "USD"}), must_be=">0.9", name="valid_currency") \
            .check(dqm.distinct_count('merchant'), must_be=">1", name="merchant_diversity")

print("✓ Financial transactions task configured with strict validation and no late data tolerance")

# Step 5: Display configuration summary
print(f"\n=== Configuration Summary ===")
print(f"Total tasks configured: {len(daq._tasks)}") # Only for demonstration, do not alter this variable!

task_status = daq.get_task_status()
for task_name, status in task_status["tasks"].items():
    criticality = "CRITICAL" if status["critical"] else "NON-CRITICAL"
    print(f"- {task_name}: {criticality}, Configured: {status['configured']}")

output_config = daq.get_output_configuration()
print(f"\n=== Output Configuration ===")
for task_name, config in output_config.items():
    print(f"- {task_name}: {config['sink_operation']}")

# Step 6: Start monitoring all tasks
print(f"\n=== Starting Multi-Source Monitoring ===")
print("Each task will run independently with its own configuration...")
print("Critical tasks will stop all monitoring if they fail.")
print("Non-critical tasks will continue running even if others fail.\n")

try:
    # This will start monitoring for all three tasks concurrently
    daq.watch_out()
    
except Exception as e:
    print(f"Monitoring stopped due to error: {e}")
    print("This could be due to a critical task failure or system error.")


# Example Output Explanation:
print("""
=== Example Demonstrates ===

1. **Multi-Source Architecture**: 
   - Three completely independent data sources
   - Different data formats (compact vs native)
   - Different windowing strategies (sliding vs tumbling)

2. **Task-Based Configuration**:
   - Each task has its own source, windowing, and quality checks
   - Critical vs non-critical task designation
   - Independent error handling and output

3. **Advanced Features**:
   - Compact data handling for IoT sensors (automatic transformation)
   - Schema validation for financial transactions
   - Different window sizes and update frequencies
   - Per-instance monitoring (sensor_id, user_id)

4. **Error Handling**:
   - Critical tasks (IoT, Financial) will stop all monitoring if they fail
   - Non-critical tasks (User Analytics) failures are logged but don't stop others
   - Detailed error reporting with task context

5. **Backward Compatibility**:
   - This new multi-task approach coexists with the original single-source API
   - Existing Stream DaQ code continues to work unchanged
""")
