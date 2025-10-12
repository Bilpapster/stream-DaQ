📤 Output Handling
==================

Stream DaQ provides flexible output handling to route quality monitoring results to your preferred destinations. This guide covers all output options, from simple console output to sophisticated alerting systems.

.. contents:: In this guide:
   :local:
   :depth: 2

Overview
--------

Stream DaQ produces three types of output:

1. **Quality Meta-Stream**: Main monitoring results with measure values and assessment outcomes
2. **Violations Stream**: Records that fail schema validation (if configured)
3. **Alerts Stream**: Real-time alerts for quality failures (if configured)

Each output type can be routed independently to different destinations.

Default Output Behavior
-----------------------

Without any configuration, Stream DaQ outputs results to the console:

.. code-block:: python

    # Default: Results printed to console
    daq = StreamDaQ().configure(
        source=data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )
    daq.check(dqm.count('events'), must_be=">100", name="volume")
    daq.watch_out()  # Prints results to console

**Console Output Example**:

.. code-block:: text

    | window_start        | window_end          | volume      |
    |---------------------|---------------------|-------------|
    | 2024-01-01 10:00:00 | 2024-01-01 10:01:00 | (150, True) |
    | 2024-01-01 10:01:00 | 2024-01-01 10:02:00 | (89, False) |

The tuple format ``(value, assessment_result)`` shows both the measured value and whether it passed the quality check.

Custom Sink Operations
----------------------

Route quality results to custom destinations using sink operations.

File Output
^^^^^^^^^^^

.. code-block:: python

    import pathway as pw

    # JSON Lines output
    def write_to_jsonlines(table):
        pw.io.jsonlines.write(table, "quality_results.jsonl")

    # CSV output
    def write_to_csv(table):
        pw.io.csv.write(table, "quality_results.csv")

    # Parquet output
    def write_to_parquet(table):
        pw.io.fs.write(table, "output/", format="parquet")

    # Configure sink
    daq.configure(
        source=data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        sink_operation=write_to_jsonlines  # Custom output
    )

Database Output
^^^^^^^^^^^^^^^

.. code-block:: python

    # PostgreSQL output
    def write_to_postgres(table):
        pw.io.postgres.write(
            table,
            postgres_settings={
                "host": "localhost",
                "port": 5432,
                "dbname": "monitoring",
                "user": "streamdaq",
                "password": "password"
            },
            table_name="quality_metrics"
        )

    # Configure database sink
    daq.configure(
        source=data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        sink_operation=write_to_postgres
    )

Message Queue Output
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Kafka output
    def write_to_kafka(table):
        pw.io.kafka.write(
            table,
            rdkafka_settings={
                "bootstrap.servers": "localhost:9092",
                "security.protocol": "plaintext"
            },
            topic="quality-metrics"
        )

HTTP/API Output
^^^^^^^^^^^^^^^

.. code-block:: python

    import requests

    def send_to_webhook(table):
        """Send results to HTTP webhook."""
        def on_change(key, row, time, is_addition):
            if is_addition:
                payload = {
                    "timestamp": row.get("window_end"),
                    "metrics": dict(row),
                    "source": "streamdaq"
                }
                try:
                    response = requests.post(
                        "https://your-webhook.com/quality-metrics",
                        json=payload,
                        timeout=10
                    )
                    response.raise_for_status()
                except requests.RequestException as e:
                    print(f"Webhook error: {e}")
        
        pw.io.subscribe(table, on_change=on_change)

    # Configure webhook sink
    daq.configure(
        source=data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        sink_operation=send_to_webhook
    )

Multi-Task Output Configuration
-------------------------------

In multi-source scenarios, each task can have independent output configuration:

.. code-block:: python

    daq = StreamDaQ()

    # Task 1: Critical system metrics to database
    critical_task = daq.new_task("critical_systems", critical=True)
    critical_task.configure(
        source=system_data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        sink_operation=write_to_postgres  # Database for critical data
    )

    # Task 2: User analytics to file
    analytics_task = daq.new_task("user_analytics", critical=False)
    analytics_task.configure(
        source=user_data,
        window=Windows.tumbling(3600),
        time_column="timestamp",
        sink_operation=write_to_jsonlines  # File for analytics
    )

    daq.watch_out()  # All tasks run with their configured outputs

Real-Time Alerting
------------------

Set up immediate alerts for quality failures:

.. code-block:: python

    # Define alert conditions
    def create_alert_handler(alert_channel="email"):
        def handle_quality_alerts(results_table):
            def on_change(key, row, time, is_addition):
                if is_addition:
                    # Check for quality failures
                    for column, value in row.items():
                        if isinstance(value, tuple) and len(value) == 2:
                            measure_value, passed = value
                            if not passed:
                                alert = {
                                    "timestamp": time,
                                    "check_name": column,
                                    "measured_value": measure_value,
                                    "status": "FAILED",
                                    "window_start": row.get("window_start"),
                                    "window_end": row.get("window_end")
                                }
                                send_alert(alert, channel=alert_channel)
            
            pw.io.subscribe(results_table, on_change=on_change)
        return handle_quality_alerts

    # Configure real-time alerting
    daq.configure(
        source=critical_data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        sink_operation=create_alert_handler("slack")
    )

Output Data Formats
-------------------

Quality Meta-Stream Format
^^^^^^^^^^^^^^^^^^^^^^^^^^

The main output contains windowed quality metrics:

.. code-block:: json

    {
        "window_start": "2024-01-01T10:00:00Z",
        "window_end": "2024-01-01T10:01:00Z",
        "instance": "user_123",
        "volume_check": [150, true],
        "avg_response": [245.5, true],
        "error_rate": [2.1, false],
        "task_name": "api_monitoring"
    }

Common Output Scenarios
-----------------------

Development and Testing
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Development: Console output with debugging
    def dev_output(table):
        # Print to console
        pw.io.subscribe(table, on_change=lambda k, r, t, a: print(f"Result: {r}"))
        
        # Also save to file for analysis
        pw.io.jsonlines.write(table, "dev_results.jsonl")

Production Monitoring
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Production: Robust, multi-destination output
    def production_output(table):
        # Primary: Metrics database
        pw.io.postgres.write(table, postgres_settings=prod_db_config, table_name="quality_metrics")
        
        # Secondary: Real-time dashboard
        pw.io.kafka.write(table, rdkafka_settings=prod_kafka_config, topic="quality-dashboard")

Troubleshooting Output Issues
-----------------------------

Common Problems
^^^^^^^^^^^^^^^

**Issue**: "Output not appearing"

**Solutions**:
- Check sink operation configuration
- Verify destination accessibility (permissions, network)
- Test with simple console output first

**Issue**: "High memory usage"

**Solutions**:
- Use streaming outputs (Kafka, database)
- Implement batching
- Reduce window sizes

Next Steps
----------

Now that you understand output handling, continue with:

- :doc:`migration-guide` - Learn about multi-source monitoring patterns
- :doc:`configuration` - Review advanced configuration options
- :doc:`measures` - Explore additional quality measures
- :doc:`assessment-functions` - Create sophisticated quality criteria