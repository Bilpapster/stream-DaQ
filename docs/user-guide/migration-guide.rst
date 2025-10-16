📈 Migration Guide: Single to Multi-Source
===========================================

This guide helps you migrate from Stream DaQ's traditional single-source monitoring to the new task-based multi-source architecture. The migration is completely optional - existing code continues to work unchanged.

.. admonition:: No Breaking Changes
   :class: tip

   **Your existing Stream DaQ code will continue to work exactly as before.** This migration guide is for users who want to take advantage of the new multi-source capabilities or prefer the clearer task-based API.

Understanding the Change
------------------------

Before: Single-Source Architecture
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Traditional approach - one StreamDaQ instance per data source
    
    # Monitor sensor data
    sensor_daq = StreamDaQ().configure(
        source=sensor_data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )
    sensor_daq.add(dqm.count('readings'), assess=">50", name="volume")
    
    # Monitor user events (separate instance required)
    user_daq = StreamDaQ().configure(
        source=user_data,
        window=Windows.tumbling(3600),
        time_column="event_time"
    )
    user_daq.add(dqm.distinct_count('action'), assess=">3", name="engagement")
    
    # Start both separately
    sensor_daq.watch_out()  # Blocks - can't run both simultaneously
    user_daq.watch_out()    # Never reached

After: Multi-Source Task Architecture
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # New approach - one StreamDaQ instance manages multiple tasks
    
    daq = StreamDaQ()
    
    # Task 1: Sensor monitoring
    sensor_task = daq.new_task("sensors")
    sensor_task.configure(
        source=sensor_data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )
    sensor_task.check(dqm.count('readings'), must_be=">50", name="volume")
    
    # Task 2: User monitoring
    user_task = daq.new_task("users")
    user_task.configure(
        source=user_data,
        window=Windows.tumbling(3600),
        time_column="event_time"
    )
    user_task.check(dqm.distinct_count('action'), must_be=">3", name="engagement")
    
    # Start both simultaneously
    daq.watch_out()  # Monitors both tasks concurrently

Migration Strategies
--------------------

Choose the migration approach that best fits your situation:

Strategy 1: No Migration (Recommended for Simple Cases)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**When to use**: Single data source, satisfied with current functionality

.. code-block:: python

    # Your existing code - no changes needed
    daq = StreamDaQ().configure(
        source=data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )
    daq.add(dqm.count('events'), assess=">10", name="count")
    daq.watch_out()

**Result**: Code works exactly as before, no migration required.

Strategy 2: Gradual Enhancement (Recommended for Most Cases)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**When to use**: Want to add new monitoring while keeping existing code

.. code-block:: python

    # Phase 1: Keep existing code, add new tasks
    daq = StreamDaQ()
    
    # Existing monitoring (backward compatible)
    daq.configure(source=legacy_data, window=Windows.tumbling(60), time_column="timestamp")
    daq.check(dqm.count('events'), must_be=">10", name="count")  # Use new method name
    
    # Add new monitoring capabilities
    new_task = daq.new_task("additional_source")
    new_task.configure(source=new_data, window=Windows.sliding(120, 30), time_column="timestamp")
    new_task.check(dqm.mean('values'), must_be="(0, 100)", name="avg")
    
    daq.watch_out()  # Monitors both legacy and new

**Benefits**:
- Minimal code changes
- Immediate access to multi-source capabilities
- Gradual learning curve

Strategy 3: Full Migration (Recommended for Complex Cases)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**When to use**: Multiple data sources, want explicit task management

.. code-block:: python

    # Before: Multiple StreamDaQ instances
    sensor_daq = StreamDaQ().configure(source=sensor_data, ...)
    user_daq = StreamDaQ().configure(source=user_data, ...)
    
    # After: Single instance with explicit tasks
    daq = StreamDaQ()
    
    sensor_task = daq.new_task("sensor_monitoring")
    sensor_task.configure(source=sensor_data, ...)
    sensor_task.check(...)
    
    user_task = daq.new_task("user_monitoring")  
    user_task.configure(source=user_data, ...)
    user_task.check(...)
    
    daq.watch_out()  # Unified execution

**Benefits**:
- Clean, explicit architecture
- Better resource management
- Enhanced error handling
- Easier debugging and monitoring

Step-by-Step Migration
----------------------

Step 1: Assess Current Usage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Identify your current Stream DaQ usage patterns:

.. code-block:: python

    # Pattern A: Single data source
    daq = StreamDaQ().configure(source=data, ...)
    daq.add(measure, ...)
    daq.watch_out()
    
    # Pattern B: Multiple separate instances
    daq1 = StreamDaQ().configure(source=data1, ...)
    daq2 = StreamDaQ().configure(source=data2, ...)
    # Running separately or in different processes

**Migration recommendation**:
- Pattern A → Strategy 1 (no migration) or Strategy 2 (gradual)
- Pattern B → Strategy 3 (full migration)

Step 2: Update Method Names (Optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Replace deprecated method names for better clarity:

.. code-block:: python

    # Old method (still works, shows deprecation warning)
    daq.add(dqm.count('events'), assess=">10", name="count")
    
    # New method (recommended)
    daq.check(dqm.count('events'), must_be=">10", name="count")

Step 3: Convert to Explicit Tasks (Optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transform implicit default task to explicit task:

.. code-block:: python

    # Before: Implicit default task
    daq = StreamDaQ().configure(source=data, window=Windows.tumbling(60), time_column="timestamp")
    daq.check(dqm.count('events'), must_be=">10", name="count")
    
    # After: Explicit task
    daq = StreamDaQ()
    main_task = daq.new_task("main_monitoring")
    main_task.configure(source=data, window=Windows.tumbling(60), time_column="timestamp")
    main_task.check(dqm.count('events'), must_be=">10", name="count")

Step 4: Add Multi-Source Capabilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Extend with additional data sources:

.. code-block:: python

    # Add second data source
    secondary_task = daq.new_task("secondary_monitoring")
    secondary_task.configure(
        source=secondary_data,
        window=Windows.sliding(300, 60),  # Different windowing
        time_column="timestamp",
        instance="device_id"  # Different grouping
    )
    secondary_task.check(dqm.mean('temperature'), must_be="(15, 35)", name="temp_check")

Step 5: Implement Error Handling
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Add critical/non-critical task designation:

.. code-block:: python

    # Critical monitoring (failure stops everything)
    critical_task = daq.new_task("critical_systems", critical=True)
    critical_task.configure(source=critical_data, ...)
    
    # Non-critical monitoring (failure logged but doesn't stop others)
    analytics_task = daq.new_task("analytics", critical=False)
    analytics_task.configure(source=analytics_data, ...)
    
    try:
        daq.watch_out()
    except CriticalTaskFailureError as e:
        print(f"Critical failure in task '{e.task_name}': {e.original_error}")
        # Implement recovery procedures

Common Migration Patterns
--------------------------

Pattern 1: IoT + Analytics
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Before: Separate monitoring
    iot_daq = StreamDaQ().configure(source=sensor_data, ...)
    analytics_daq = StreamDaQ().configure(source=user_data, ...)
    
    # After: Unified monitoring
    daq = StreamDaQ()
    
    # IoT sensors (critical for safety)
    iot_task = daq.new_task("iot_sensors", critical=True)
    iot_task.configure(
        source=sensor_data,
        window=Windows.sliding(300, 60),
        compact_data=CompactData()  # Handle compact sensor data
    )
    
    # User analytics (non-critical)
    analytics_task = daq.new_task("user_analytics", critical=False)
    analytics_task.configure(
        source=user_data,
        window=Windows.tumbling(3600)
    )

Pattern 2: Financial + Operational
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Before: Separate critical systems
    payment_daq = StreamDaQ().configure(source=payments, ...)
    ops_daq = StreamDaQ().configure(source=operations, ...)
    
    # After: Unified with proper criticality
    daq = StreamDaQ()
    
    # Payment processing (critical)
    payment_task = daq.new_task("payments", critical=True)
    payment_task.configure(
        source=payments,
        window=Windows.tumbling(60),
        wait_for_late=0,  # No tolerance for late payments
        schema_validator=payment_validator
    )
    
    # Operational metrics (non-critical)
    ops_task = daq.new_task("operations", critical=False)
    ops_task.configure(
        source=operations,
        window=Windows.tumbling(300)
    )

Pattern 3: Multi-Environment Monitoring
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Before: Environment-specific instances
    prod_daq = StreamDaQ().configure(source=prod_data, ...)
    staging_daq = StreamDaQ().configure(source=staging_data, ...)
    
    # After: Unified cross-environment monitoring
    daq = StreamDaQ()
    
    # Production monitoring (critical)
    prod_task = daq.new_task("production", critical=True)
    prod_task.configure(source=prod_data, ...)
    
    # Staging monitoring (non-critical)
    staging_task = daq.new_task("staging", critical=False)
    staging_task.configure(source=staging_data, ...)

Migration Checklist
--------------------

Use this checklist to ensure a smooth migration:

**Pre-Migration**
- [ ] Identify current Stream DaQ usage patterns
- [ ] Determine which data sources are critical vs non-critical
- [ ] Plan task naming strategy
- [ ] Review error handling requirements

**During Migration**
- [ ] Update method names (`add` → `check`)
- [ ] Convert to explicit tasks if desired
- [ ] Add task criticality designation
- [ ] Test error handling scenarios
- [ ] Verify all data sources are monitored

**Post-Migration**
- [ ] Monitor task execution and performance
- [ ] Validate error isolation works correctly
- [ ] Update documentation and runbooks
- [ ] Train team on new task-based concepts

**Testing Your Migration**

.. code-block:: python

    # Test task status and configuration
    status = daq.get_task_status()
    print(f"Total tasks: {status['total_tasks']}")
    
    for task_name, task_info in status["tasks"].items():
        print(f"Task '{task_name}': Critical={task_info['critical']}, Configured={task_info['configured']}")
    
    # Test output configuration
    output_config = daq.get_output_configuration()
    for task_name, config in output_config.items():
        print(f"Task '{task_name}' output: {config['sink_operation']}")

Troubleshooting Migration Issues
---------------------------------

Issue: Deprecation Warnings
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Seeing warnings about deprecated `add()` method

**Solution**: Replace with `check()` for clarity

.. code-block:: python

    # Replace this
    daq.add(dqm.count('events'), assess=">10", name="count")
    
    # With this
    daq.check(dqm.count('events'), must_be=">10", name="count")

Issue: Task Name Conflicts
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Error about duplicate task names

**Solution**: Use unique task names or let Stream DaQ auto-generate

.. code-block:: python

    # Problem: duplicate names
    task1 = daq.new_task("monitoring")
    task2 = daq.new_task("monitoring")  # Error!
    
    # Solution: unique names
    task1 = daq.new_task("sensor_monitoring")
    task2 = daq.new_task("user_monitoring")
    
    # Or auto-generate
    task1 = daq.new_task()  # "task_1"
    task2 = daq.new_task()  # "task_2"

Issue: Critical Task Failures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: All monitoring stops when one task fails

**Solution**: Review task criticality designation

.. code-block:: python

    # Problem: everything marked critical
    task1 = daq.new_task("analytics", critical=True)  # Should be False
    task2 = daq.new_task("payments", critical=True)   # Correctly True
    
    # Solution: appropriate criticality
    task1 = daq.new_task("analytics", critical=False)  # Non-critical
    task2 = daq.new_task("payments", critical=True)    # Critical

Issue: Performance Concerns
^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Worried about resource usage with multiple tasks

**Solution**: Tasks are lightweight and share resources efficiently

.. code-block:: python

    # Efficient: Multiple tasks in one instance
    daq = StreamDaQ()
    task1 = daq.new_task("source1")
    task2 = daq.new_task("source2")
    task3 = daq.new_task("source3")
    daq.watch_out()  # Coordinated execution
    
    # Less efficient: Multiple separate instances
    daq1 = StreamDaQ().configure(source=source1, ...)
    daq2 = StreamDaQ().configure(source=source2, ...)
    daq3 = StreamDaQ().configure(source=source3, ...)

Getting Help
------------

If you encounter issues during migration:

1. **Check Examples**: Review ``examples/mixed_api_usage.py`` for migration patterns
2. **Test Incrementally**: Migrate one task at a time
3. **Use Task Status**: Monitor task health with ``daq.get_task_status()``
4. **Verify Configuration**: Check output setup with ``daq.get_output_configuration()``

The task-based architecture is designed to enhance Stream DaQ's capabilities while maintaining the simplicity and reliability you expect. Take your time with migration and leverage the backward compatibility to transition at your own pace.

|made_with_love|