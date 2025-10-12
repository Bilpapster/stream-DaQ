# pip install streamdaq

"""
Critical Task Handling Example

This example demonstrates how Stream DaQ handles critical vs non-critical task failures,
showing the error isolation and recovery mechanisms in multi-source monitoring scenarios.
"""

import pathway as pw
from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows


# Step 1: Create data sources with different failure scenarios

class ReliableSource(pw.io.python.ConnectorSubject):
    """A reliable data source that works consistently."""
    
    def run(self):
        for i in range(1, 11):
            self.next(
                timestamp=i,
                sensor_id=f"reliable_{i % 3}",
                value=100 + i * 5,
                status="OK"
            )

class UnreliableSource(pw.io.python.ConnectorSubject):
    """An unreliable data source that occasionally fails."""
    
    def run(self):
        for i in range(1, 8):
            if i == 5:  # Simulate failure at record 5
                raise ConnectionError("Network timeout - sensor unreachable")
            
            self.next(
                timestamp=i,
                sensor_id=f"unreliable_{i % 2}",
                value=200 + i * 3,
                status="WARNING" if i % 3 == 0 else "OK"
            )

class CriticalSystemSource(pw.io.python.ConnectorSubject):
    """A critical system that must not fail."""
    
    def run(self):
        for i in range(1, 6):
            # This source is reliable but we'll simulate what happens
            # if a critical task has issues
            self.next(
                timestamp=i,
                system_id="critical_system",
                cpu_usage=50 + i * 2,
                memory_usage=60 + i * 3,
                disk_usage=30 + i
            )


# Step 2: Create data tables
reliable_schema = pw.schema_from_dict({
    "timestamp": int,
    "sensor_id": str, 
    "value": int,
    "status": str
})

unreliable_schema = pw.schema_from_dict({
    "timestamp": int,
    "sensor_id": str,
    "value": int, 
    "status": str
})

critical_schema = pw.schema_from_dict({
    "timestamp": int,
    "system_id": str,
    "cpu_usage": int,
    "memory_usage": int,
    "disk_usage": int
})

reliable_data = pw.io.python.read(ReliableSource(), schema=reliable_schema)
unreliable_data = pw.io.python.read(UnreliableSource(), schema=unreliable_schema)
critical_data = pw.io.python.read(CriticalSystemSource(), schema=critical_schema)


print("=== Critical Task Handling Example ===")
print("Demonstrating error isolation and critical task behavior\n")

# Step 3: Configure tasks with different criticality levels

daq = StreamDaQ()

# Task 1: Reliable monitoring (Non-critical)
print("1. Configuring RELIABLE MONITORING (Non-critical)")
reliable_task = daq.new_task("reliable_monitoring", critical=False)
reliable_task.configure(
    source=reliable_data,
    window=Windows.tumbling(3),
    time_column="timestamp",
    instance="sensor_id"
)
reliable_task.check(dqm.count('value'), must_be=">0", name="data_availability") \
             .check(dqm.mean('value'), must_be="(100, 200)", name="value_range")

print("✓ Reliable task configured (will always work)")

# Task 2: Unreliable monitoring (Non-critical) 
print("\n2. Configuring UNRELIABLE MONITORING (Non-critical)")
unreliable_task = daq.new_task("unreliable_monitoring", critical=False)
unreliable_task.configure(
    source=unreliable_data,
    window=Windows.tumbling(2),
    time_column="timestamp",
    instance="sensor_id"
)
unreliable_task.check(dqm.count('value'), must_be=">0", name="unreliable_count") \
               .check(dqm.max('value'), must_be="<300", name="max_value_check")

print("✓ Unreliable task configured (will fail but won't stop others)")

# Task 3: Critical system monitoring (Critical)
print("\n3. Configuring CRITICAL SYSTEM MONITORING (Critical)")
critical_task = daq.new_task("critical_system", critical=True)
critical_task.configure(
    source=critical_data,
    window=Windows.tumbling(2),
    time_column="timestamp"
)
critical_task.check(dqm.max('cpu_usage'), must_be="<90", name="cpu_threshold") \
             .check(dqm.max('memory_usage'), must_be="<85", name="memory_threshold") \
             .check(dqm.max('disk_usage'), must_be="<80", name="disk_threshold")

print("✓ Critical task configured (failure will stop all monitoring)")

# Step 4: Show task configuration summary
print(f"\n=== Task Configuration Summary ===")
status = daq.get_task_status()

for task_name, task_info in status["tasks"].items():
    criticality = "🔴 CRITICAL" if task_info["critical"] else "🟡 NON-CRITICAL"
    print(f"- {task_name}: {criticality}")

print(f"\nTotal tasks: {status['total_tasks']}")

# Step 5: Demonstrate different failure scenarios

print(f"\n=== Failure Scenario 1: Non-Critical Task Failure ===")
print("Starting monitoring with unreliable task that will fail...")

try:
    # This will show how non-critical task failures are handled
    daq.watch_out()
    
except Exception as e:
    print(f"Monitoring completed with some task failures: {e}")
    print("Non-critical task failures were logged but didn't stop other tasks")

# Step 6: Demonstrate critical task failure scenario
print(f"\n=== Failure Scenario 2: Critical Task Failure (Simulation) ===")
print("Now simulating what happens when a critical task fails...")

# Create a new StreamDaQ instance for critical failure demo
daq_critical = StreamDaQ()

# Add the same reliable task
reliable_task_2 = daq_critical.new_task("reliable_backup", critical=False)
reliable_task_2.configure(
    source=reliable_data,
    window=Windows.tumbling(3),
    time_column="timestamp"
)
reliable_task_2.check(dqm.count('value'), name="backup_count")

# Add a critical task that will fail
class FailingCriticalSource(pw.io.python.ConnectorSubject):
    def run(self):
        for i in range(3):
            if i == 1:  # Fail early
                raise RuntimeError("Critical system failure - security breach detected!")
            self.next(timestamp=i, critical_value=i)

failing_critical_data = pw.io.python.read(
    FailingCriticalSource(), 
    schema=pw.schema_from_dict({"timestamp": int, "critical_value": int})
)

critical_failing_task = daq_critical.new_task("critical_security", critical=True)
critical_failing_task.configure(
    source=failing_critical_data,
    window=Windows.tumbling(2),
    time_column="timestamp"
)
critical_failing_task.check(dqm.count('critical_value'), name="security_check")

print("Critical task configured to fail...")

try:
    daq_critical.watch_out()
except Exception as e:
    print(f"🚨 CRITICAL FAILURE: {e}")
    print("All monitoring stopped due to critical task failure!")

# Step 7: Best practices summary
print(f"""
=== Critical Task Handling Best Practices ===

1. **Task Criticality Guidelines**:
   🔴 CRITICAL tasks should be used for:
   - Safety-critical systems (medical devices, industrial control)
   - Security monitoring (fraud detection, intrusion detection)  
   - Financial systems (payment processing, trading)
   - Core infrastructure (database health, network connectivity)

   🟡 NON-CRITICAL tasks should be used for:
   - Analytics and reporting
   - Performance monitoring
   - User behavior tracking
   - Experimental features

2. **Error Handling Behavior**:
   ✓ Non-critical task failures are logged but don't stop other tasks
   ✓ Critical task failures immediately stop all monitoring
   ✓ Detailed error context is provided for debugging
   ✓ Task isolation prevents cascading failures

3. **Monitoring Strategy**:
   ✓ Start with fewer critical tasks to avoid unnecessary shutdowns
   ✓ Use critical designation sparingly for truly essential systems
   ✓ Monitor critical task health separately
   ✓ Have fallback procedures for critical task failures

4. **Error Recovery**:
   ✓ Non-critical tasks can be restarted independently
   ✓ Critical task failures require manual intervention
   ✓ Consider implementing retry logic for transient failures
   ✓ Use circuit breaker patterns for external dependencies

=== Example Output Interpretation ===

When you see:
- "Task 'X' failed with error: Y" → Non-critical failure, monitoring continues
- "CRITICAL TASK FAILURE: Task 'X' failed" → All monitoring stops immediately
- "Continuing with remaining tasks..." → Error isolation working correctly
- "Stopping all monitoring due to critical task failure" → Critical failure detected

This architecture ensures that your most important monitoring never gets
disrupted by less critical system failures, while still providing comprehensive
visibility into all your data streams.
""")


# Step 8: Recovery and restart example
print(f"\n=== Task Recovery Example ===")
print("Demonstrating how to recover from failures...")

# Create a new instance for recovery demo
daq_recovery = StreamDaQ()

# Add only the working tasks
recovery_task = daq_recovery.new_task("recovery_monitoring", critical=False)
recovery_task.configure(
    source=reliable_data,
    window=Windows.tumbling(3),
    time_column="timestamp"
)
recovery_task.check(dqm.count('value'), name="recovery_count")

print("Recovery monitoring configured with only reliable components")
print("This demonstrates how to restart monitoring after addressing failures")

try:
    daq_recovery.watch_out()
    print("✓ Recovery monitoring completed successfully")
except Exception as e:
    print(f"Recovery failed: {e}")
