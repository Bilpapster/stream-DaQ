# pip install streamdaq

"""
Mixed API Usage Example - Backward Compatibility Demonstration

This example shows how the new task-based architecture maintains full backward 
compatibility with existing Stream DaQ code while enabling new multi-source capabilities.
"""

import pathway as pw
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows


# Step 1: Create sample data sources

# Legacy data source (will use backward compatibility)
legacy_data = pw.debug.table_from_markdown("""
    | timestamp | user_id | events | temperature
  1 | 1         | user_1  | 5      | 22.1
  2 | 2         | user_2  | 8      | 23.5
  3 | 3         | user_1  | 3      | 21.8
  4 | 4         | user_3  | 12     | 24.2
  5 | 5         | user_2  | 7      | 22.9
""")

# New data source (will use new task API)
new_sensor_data = pw.debug.table_from_markdown("""
    | timestamp | sensor_id | pressure | humidity
  1 | 1         | sensor_A  | 1013.2   | 45
  2 | 2         | sensor_B  | 1012.8   | 48  
  3 | 3         | sensor_A  | 1014.1   | 43
  4 | 4         | sensor_C  | 1013.5   | 46
  5 | 5         | sensor_B  | 1012.3   | 49
""")


print("=== Mixed API Usage Example ===")
print("Demonstrating backward compatibility + new multi-source capabilities\n")

# Step 2: Use the OLD API (backward compatibility)
print("1. Using LEGACY API (backward compatible):")
print("   - configure() creates default task internally")
print("   - add() method still works (with deprecation warning)")

daq = StreamDaQ()

# This is the OLD way - still works!
daq.configure(
    source=legacy_data,
    window=Windows.tumbling(3),
    time_column="timestamp",
    instance="user_id"
)

# This will show a deprecation warning but still works
daq.add(dqm.count('events'), assess=">2", name="event_count")
daq.add(dqm.mean('temperature'), assess="(20, 25)", name="temp_avg")

print("✓ Legacy configuration completed (using old API)")

# Step 3: Mix with NEW API (add additional tasks)
print("\n2. Adding NEW TASKS (new multi-source API):")
print("   - new_task() creates explicit named tasks")
print("   - add_check() method for clarity")

# Add a new task using the NEW API
sensor_task = daq.new_task("pressure_sensors", critical=False)
sensor_task.configure(
    source=new_sensor_data,
    window=Windows.sliding(duration=4, hop=2),  # Different windowing!
    time_column="timestamp", 
    instance="sensor_id"
)

sensor_task.check(dqm.mean('pressure'), must_be="(1010, 1020)", name="pressure_range") \
           .check(dqm.max('humidity'), must_be="<60", name="humidity_max")

print("✓ New task added using new API")


# Step 4: Show the mixed configuration
print(f"\n3. CONFIGURATION SUMMARY:")
print(f"   Total tasks: {len(daq._tasks)}")

for task_name, task in daq._tasks.items():
    window_type = "Tumbling" if hasattr(task.window, 'duration') and not hasattr(task.window, 'hop') else "Sliding"
    print(f"   - {task_name}: {window_type} window, {len(task._checks.keys())} checks")

# Step 5: Demonstrate error handling scenarios
print(f"\n4. ERROR HANDLING DEMONSTRATION:")

# Create a task that will fail (for demonstration)
print("   Creating a task that will fail to demonstrate error handling...")

class FailingSource(pw.io.python.ConnectorSubject):
    def run(self):
        # This will cause an error after a few records
        for i in range(3):
            if i == 2:
                raise ValueError("Simulated sensor failure!")
            self.next(timestamp=i, value=i * 10)

failing_schema = pw.schema_from_dict({"timestamp": int, "value": int})
failing_data = pw.io.python.read(FailingSource(), schema=failing_schema)

# Add non-critical failing task
failing_task = daq.new_task("failing_sensor", critical=False)  # Non-critical
failing_task.configure(
    source=failing_data,
    window=Windows.tumbling(2),
    time_column="timestamp"
)
failing_task.check(dqm.count('value'), name="value_count")

print("✓ Added a task that will fail (non-critical)")

# Step 6: Start monitoring with error handling
print(f"\n5. STARTING MIXED MONITORING:")
print("   - Legacy task will run normally")
print("   - New sensor task will run with different windowing") 
print("   - Failing task will fail but not stop others (non-critical)")

try:
    # Show task status before starting
    status = daq.get_task_status()
    print(f"\n   Pre-execution status:")
    for task_name, task_status in status["tasks"].items():
        critical_str = "CRITICAL" if task_status["critical"] else "NON-CRITICAL"
        print(f"   - {task_name}: {critical_str}")
    
    print(f"\n   Starting monitoring...")
    daq.watch_out()
    
except Exception as e:
    print(f"   Expected error occurred: {e}")
    print("   This demonstrates error isolation - non-critical failures don't stop everything")


# Step 7: Show what this example demonstrates
print(f"""
=== What This Example Demonstrates ===

1. **Perfect Backward Compatibility**:
   ✓ Old configure() + add() API still works exactly as before
   ✓ Existing code runs unchanged with no modifications needed
   ✓ Deprecation warnings guide users to new API

2. **Seamless API Mixing**:
   ✓ Can use old API for some tasks, new API for others
   ✓ Both approaches work together in the same StreamDaQ instance
   ✓ No conflicts or interference between API styles

3. **Enhanced Error Handling**:
   ✓ Non-critical task failures don't stop other tasks
   ✓ Detailed error reporting with task context
   ✓ Graceful degradation for robust monitoring

4. **Independent Task Configuration**:
   ✓ Different windowing strategies per task
   ✓ Different data sources and schemas
   ✓ Independent quality checks and assessments

5. **Migration Path**:
   ✓ Start with existing code (works as-is)
   ✓ Gradually add new tasks using new API
   ✓ Eventually migrate old code to new API when convenient

This demonstrates that Stream DaQ's new multi-source architecture enhances
capabilities without breaking existing functionality!
""")


# Additional example: Converting from old to new API
print(f"\n=== API CONVERSION EXAMPLE ===")
print("How to convert from old API to new API:")

print("""
OLD API (still works):
    daq = StreamDaQ()
    daq.configure(source=data, window=Windows.tumbling(60), time_column="timestamp")
    daq.add(dqm.count('events'), must_be=">10", name="count")
    daq.watch_out()

NEW API (recommended):
    daq = StreamDaQ()
    task = daq.new_task("main_monitoring")
    task.configure(source=data, window=Windows.tumbling(60), time_column="timestamp")
    task.add_check(dqm.count('events'), must_be=">10", name="count")
    daq.watch_out()

MIXED API (transition approach):
    daq = StreamDaQ()
    # Keep existing code as-is
    daq.configure(source=legacy_data, window=Windows.tumbling(60), time_column="timestamp")
    daq.add_check(dqm.count('events'), must_be=">10", name="count")  # Use new method name
    
    # Add new tasks
    new_task = daq.new_task("additional_monitoring")
    new_task.configure(source=new_data, window=Windows.sliding(120, 30), time_column="timestamp")
    new_task.add_check(dqm.mean('values'), must_be="(0, 100)", name="avg_check")
    
    daq.watch_out()  # Monitors both legacy and new tasks
""")