🎯 Multi-Source Tasks
=====================

Stream DaQ's task-based architecture enables monitoring multiple independent data sources within a single StreamDaQ instance. This powerful feature eliminates the need to manage multiple monitoring processes while providing complete isolation and flexibility for each data source.

.. admonition:: Why Multi-Source Monitoring?
   :class: tip

   In real-world scenarios, you often need to monitor diverse data streams simultaneously: IoT sensors, user events, financial transactions, system metrics. The task-based architecture lets you handle all of these with different configurations, windowing strategies, and quality checks from a unified interface.

Understanding Tasks
-------------------

A **Task** in Stream DaQ represents a complete data quality monitoring configuration for a single data source. Each task encapsulates:

- **Data Source**: The stream to monitor
- **Windowing Strategy**: How to group data over time
- **Quality Measures**: What to check and assess
- **Schema Validation**: Optional data validation rules
- **Output Configuration**: Where to send results
- **Error Handling**: Critical vs non-critical designation

.. grid:: 1 1 2 2
    :gutter: 4

    .. grid-item-card:: **Single-Source (Traditional)**
        :class-header: bg-info text-white

        One StreamDaQ instance monitors one data source with one configuration.

    .. grid-item-card:: **Multi-Source (Task-Based)**
        :class-header: bg-success text-white

        One StreamDaQ instance orchestrates multiple tasks, each monitoring different sources with independent configurations.

Task Independence
-----------------

Tasks operate completely independently:

**Independent Configuration**
    Each task has its own windowing, schema validation, compact data handling, and sink operations.

**Independent Execution**
    Tasks run concurrently without interfering with each other's processing.

**Independent Error Handling**
    Non-critical task failures don't affect other tasks. Critical task failures can stop all monitoring.

**Independent Output**
    Each task produces its own quality monitoring results at its own intervals.

API Evolution: Single to Multi-Source
--------------------------------------

Stream DaQ's API has evolved to support both single-source and multi-source scenarios while maintaining full backward compatibility.

### Traditional Single-Source API

.. code-block:: python

    # The original way (still works!)
    daq = StreamDaQ().configure(
        source=sensor_data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )
    daq.add(dqm.count('readings'), assess=">50", name="volume")
    daq.watch_out()

### New Multi-Source Task API

.. code-block:: python

    # The new way - explicit tasks
    daq = StreamDaQ()
    
    # Task 1: IoT sensors
    iot_task = daq.new_task("iot_monitoring")
    iot_task.configure(
        source=sensor_data,
        window=Windows.sliding(300, 60),  # Different windowing!
        time_column="sensor_timestamp",
        compact_data=CompactData()
    )
    iot_task.check(dqm.count('temperature'), must_be=">50", name="temp_volume")
    
    # Task 2: User events  
    user_task = daq.new_task("user_analytics")
    user_task.configure(
        source=user_data,
        window=Windows.tumbling(3600),  # Hourly windows
        time_column="event_time",
        instance="user_id"
    )
    user_task.check(dqm.distinct_count('action'), must_be=">3", name="engagement")
    
    # Start monitoring all tasks
    daq.watch_out()

### Mixed API Usage (Transition Approach)

.. code-block:: python

    # Combine old and new approaches
    daq = StreamDaQ()
    
    # Keep existing code (backward compatible)
    daq.configure(source=legacy_data, window=Windows.tumbling(60), time_column="timestamp")
    daq.check(dqm.count('events'), must_be=">10", name="legacy_count")
    
    # Add new tasks
    new_task = daq.new_task("additional_monitoring")
    new_task.configure(source=new_data, window=Windows.sliding(120, 30), time_column="timestamp")
    new_task.check(dqm.mean('values'), must_be="(0, 100)", name="avg_check")
    
    daq.watch_out()  # Monitors both legacy and new tasks

Task Naming and Management
--------------------------

### Automatic Task Naming

If you don't provide a name, Stream DaQ automatically generates unique identifiers:

.. code-block:: python

    daq = StreamDaQ()
    task1 = daq.new_task()  # Automatically named "task_1"
    task2 = daq.new_task()  # Automatically named "task_2"
    task3 = daq.new_task("custom_name")  # Explicitly named

### Task Management Operations

.. code-block:: python

    # List all tasks
    task_names = daq.list_tasks()
    print(f"Configured tasks: {task_names}")
    
    # Get specific task
    iot_task = daq.get_task("iot_monitoring")
    
    # Remove task
    daq.remove_task("old_task")
    
    # Get task status
    status = daq.get_task_status()
    print(f"Total tasks: {status['total_tasks']}")

Critical vs Non-Critical Tasks
------------------------------

Tasks can be designated as **critical** or **non-critical**, affecting error handling behavior:

.. grid:: 1 1 2 2
    :gutter: 4

    .. grid-item-card:: **Critical Tasks** 🔴
        :class-header: bg-danger text-white

        Failure stops ALL monitoring immediately. Use for safety-critical, security, or financial systems.

    .. grid-item-card:: **Non-Critical Tasks** 🟡
        :class-header: bg-warning text-dark

        Failure is logged but other tasks continue. Use for analytics, reporting, or experimental features.

### Critical Task Examples

.. code-block:: python

    # Financial transaction monitoring (critical)
    finance_task = daq.new_task("transactions", critical=True)
    finance_task.configure(
        source=transaction_data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        wait_for_late=0,  # No tolerance for late financial data
        schema_validator=strict_validator
    )
    
    # User analytics (non-critical)
    analytics_task = daq.new_task("user_behavior", critical=False)
    analytics_task.configure(
        source=user_data,
        window=Windows.tumbling(3600),
        time_column="timestamp"
    )

### Error Handling Behavior

.. code-block:: python

    try:
        daq.watch_out()
    except CriticalTaskFailureError as e:
        print(f"Critical task '{e.task_name}' failed: {e.original_error}")
        print("All monitoring stopped for safety")
        # Implement recovery procedures
    except Exception as e:
        print(f"Non-critical task failure: {e}")
        print("Other tasks continue running")

Real-World Multi-Source Scenarios
----------------------------------

### Smart City Monitoring

.. code-block:: python

    daq = StreamDaQ()
    
    # Environmental sensors (critical for public safety)
    env_task = daq.new_task("environmental", critical=True)
    env_task.configure(
        source=sensor_data,
        window=Windows.sliding(300, 60),
        compact_data=CompactData(),
        time_column="sensor_timestamp"
    )
    env_task.check(dqm.missing_count('air_quality'), must_be="<5", name="air_quality_availability")
    
    # Traffic monitoring (non-critical)
    traffic_task = daq.new_task("traffic", critical=False)
    traffic_task.configure(
        source=traffic_data,
        window=Windows.tumbling(600),
        time_column="timestamp"
    )
    traffic_task.check(dqm.count('vehicles'), must_be="(10, 1000)", name="traffic_volume")

### E-commerce Platform

.. code-block:: python

    daq = StreamDaQ()
    
    # Payment processing (critical)
    payment_task = daq.new_task("payments", critical=True)
    payment_task.configure(
        source=payment_stream,
        window=Windows.tumbling(60),
        schema_validator=payment_validator,
        wait_for_late=0
    )
    
    # User behavior analytics (non-critical)
    behavior_task = daq.new_task("user_analytics", critical=False)
    behavior_task.configure(
        source=clickstream_data,
        window=Windows.session(1800),  # 30-minute sessions
        instance="user_id"
    )
    
    # Inventory monitoring (critical)
    inventory_task = daq.new_task("inventory", critical=True)
    inventory_task.configure(
        source=inventory_updates,
        window=Windows.tumbling(300)
    )

Migration Guide
---------------

### Step 1: Assess Current Usage

.. code-block:: python

    # Current single-source code
    daq = StreamDaQ().configure(source=data, window=Windows.tumbling(60), time_column="timestamp")
    daq.add(dqm.count('events'), assess=">10", name="count")
    daq.watch_out()

### Step 2: Gradual Migration

.. code-block:: python

    # Phase 1: Keep existing code, add new tasks
    daq = StreamDaQ()
    daq.configure(source=legacy_data, window=Windows.tumbling(60), time_column="timestamp")
    daq.check(dqm.count('events'), must_be=">10", name="count")  # Use new method name
    
    # Add new monitoring tasks
    new_task = daq.new_task("additional_source")
    new_task.configure(source=new_data, window=Windows.sliding(120, 30), time_column="timestamp")
    new_task.check(dqm.mean('values'), must_be="(0, 100)", name="avg")
    
    daq.watch_out()

### Step 3: Full Migration

.. code-block:: python

    # Phase 2: Convert to explicit tasks
    daq = StreamDaQ()
    
    # Convert legacy code to explicit task
    legacy_task = daq.new_task("legacy_monitoring")
    legacy_task.configure(source=legacy_data, window=Windows.tumbling(60), time_column="timestamp")
    legacy_task.check(dqm.count('events'), must_be=">10", name="count")
    
    # Additional tasks
    new_task = daq.new_task("new_monitoring")
    new_task.configure(source=new_data, window=Windows.sliding(120, 30), time_column="timestamp")
    new_task.check(dqm.mean('values'), must_be="(0, 100)", name="avg")
    
    daq.watch_out()

Best Practices
--------------

### Task Organization

.. admonition:: ✅ Good Practices
   :class: tip

   - **Use descriptive task names**: "iot_sensors", "user_events", "financial_transactions"
   - **Group related monitoring**: Keep similar data sources in the same task
   - **Separate by criticality**: Don't mix critical and non-critical monitoring in the same task
   - **Consider windowing alignment**: Tasks with similar time requirements can share window strategies

### Error Handling Strategy

.. admonition:: ⚠️ Critical Task Guidelines
   :class: warning

   - **Use sparingly**: Only mark truly essential systems as critical
   - **Have fallbacks**: Prepare recovery procedures for critical task failures
   - **Monitor health**: Track critical task performance separately
   - **Test failure scenarios**: Verify error handling works as expected

### Performance Considerations

.. code-block:: python

    # Efficient: Batch similar monitoring
    sensor_task = daq.new_task("all_sensors")
    sensor_task.configure(
        source=combined_sensor_data,
        window=Windows.tumbling(60),
        instance="sensor_type"  # Group by sensor type
    )
    
    # Less efficient: Too many small tasks
    # Avoid creating dozens of tasks for similar data

What's Next?
------------

Now that you understand multi-source tasks:

- 💡 **See examples**: :doc:`../examples/index` - Real-world multi-source monitoring scenarios
- 📏 **Learn measures**: :doc:`measures-and-assessments` - Quality checks that work across all tasks
- 🪟 **Understand windowing**: :doc:`stream-windows` - How different tasks can use different windowing strategies
- ⚡ **Production deployment**: :doc:`real-time-monitoring` - Scaling multi-source monitoring

The task-based architecture transforms Stream DaQ from a single-source monitoring tool into a comprehensive multi-source orchestration platform, all while maintaining the simplicity and power that makes Stream DaQ effective.

|made_with_love|