⚙️ Configuration
================

This guide covers how to configure Stream DaQ for your specific monitoring needs. You'll learn how to set up data sources, configure windowing strategies, handle different data formats, and optimize performance.

.. contents:: In this guide:
   :local:
   :depth: 2

Basic Configuration
-------------------

Every Stream DaQ monitoring setup follows the same three-step pattern:

1. **Configure** the monitoring parameters
2. **Define** quality checks
3. **Start** monitoring

Single-Source Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    import pathway as pw

    # Step 1: Configure monitoring
    daq = StreamDaQ().configure(
        source=your_data_stream,           # Your data source
        window=Windows.tumbling(60),       # 60-second windows
        time_column="timestamp",           # Time column name
        instance="user_id"                 # Optional: group by user
    )

    # Step 2: Define quality checks
    daq.check(dqm.count('events'), must_be=">10", name="volume_check")

    # Step 3: Start monitoring
    daq.watch_out()

Multi-Source Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

For monitoring multiple data sources simultaneously:

.. code-block:: python

    # Create StreamDaQ instance
    daq = StreamDaQ()

    # Configure first data source
    sensor_task = daq.new_task("sensors", critical=True)
    sensor_task.configure(
        source=sensor_data,
        window=Windows.sliding(300, 60),   # 5-min sliding windows
        time_column="sensor_timestamp"
    )
    sensor_task.check(dqm.mean('temperature'), must_be="(15, 35)", name="temp_range")

    # Configure second data source
    user_task = daq.new_task("user_events", critical=False)
    user_task.configure(
        source=user_data,
        window=Windows.tumbling(3600),     # 1-hour tumbling windows
        time_column="event_time",
        instance="user_id"
    )
    user_task.check(dqm.distinct_count('action'), must_be=">3", name="engagement")

    # Start monitoring all tasks
    daq.watch_out()

Data Sources
------------

Stream DaQ works with any Pathway data source. Here are common patterns:

File-Based Sources
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # JSON Lines files
    data = pw.io.jsonlines.read("data/events.jsonl", schema=your_schema)

    # CSV files
    data = pw.io.csv.read("data/metrics.csv", schema=your_schema)

    # Parquet files
    data = pw.io.fs.read("data/", format="parquet", schema=your_schema)

Streaming Sources
^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Kafka streams
    data = pw.io.kafka.read(
        rdkafka_settings={
            "bootstrap.servers": "localhost:9092",
            "group.id": "streamdaq-consumer",
            "auto.offset.reset": "latest"
        },
        topic="data-quality-events",
        schema=your_schema
    )

    # Custom Python connectors
    class CustomSource(pw.io.python.ConnectorSubject):
        def run(self):
            # Your custom data generation logic
            for i in range(100):
                self.next(timestamp=i, value=i*2, status="OK")

    data = pw.io.python.read(CustomSource(), schema=your_schema)

Database Sources
^^^^^^^^^^^^^^^^

.. code-block:: python

    # PostgreSQL
    data = pw.io.postgres.read(
        host="localhost",
        port=5432,
        dbname="monitoring",
        user="streamdaq",
        password="password",
        table="events"
    )

Windowing Strategies
--------------------

Choose the right windowing strategy for your monitoring needs:

Tumbling Windows
^^^^^^^^^^^^^^^^

**Best for**: Periodic reports, batch processing, non-overlapping analysis

.. code-block:: python

    # 5-minute non-overlapping windows
    window=Windows.tumbling(300)

    # Hourly reports
    window=Windows.tumbling(3600)

**Use cases**:
- Hourly transaction summaries
- Daily user activity reports
- Batch quality assessments

Sliding Windows
^^^^^^^^^^^^^^^

**Best for**: Real-time monitoring, trend detection, continuous analysis

.. code-block:: python

    # 10-minute windows, updated every 2 minutes
    window=Windows.sliding(duration=600, hop=120)

    # 1-hour windows, updated every 15 minutes
    window=Windows.sliding(duration=3600, hop=900)

**Use cases**:
- Real-time anomaly detection
- Continuous performance monitoring
- Moving averages and trends

Session Windows
^^^^^^^^^^^^^^^

**Best for**: User behavior analysis, activity-based grouping

.. code-block:: python

    # 30-minute session timeout
    window=Windows.session(1800)

**Use cases**:
- User session analysis
- Activity-based quality checks
- Behavioral pattern detection

Time Handling
-------------

Time Column Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Integer timestamps (Unix epoch)
    daq.configure(
        time_column="timestamp",
        time_format=None  # No parsing needed
    )

    # String timestamps
    daq.configure(
        time_column="created_at",
        time_format="%Y-%m-%d %H:%M:%S"  # Parse format
    )

    # ISO format timestamps
    daq.configure(
        time_column="event_time",
        time_format="%Y-%m-%dT%H:%M:%S.%fZ"
    )

Late Data Handling
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Wait 30 seconds for late arrivals
    daq.configure(
        wait_for_late=30,
        # ... other config
    )

    # No tolerance for late data (real-time systems)
    daq.configure(
        wait_for_late=0,
        # ... other config
    )

    # Custom late data handling
    from datetime import timedelta
    daq.configure(
        wait_for_late=timedelta(minutes=5),
        # ... other config
    )

Data Formats
------------

Native Data Format
^^^^^^^^^^^^^^^^^^

Standard row-based data where each record contains individual field values:

.. code-block:: python

    # Native format - each record has individual fields
    {
        "timestamp": 1234567890,
        "user_id": "user_123",
        "temperature": 23.5,
        "humidity": 65.2
    }

    # No special configuration needed
    daq.configure(
        source=native_data,
        window=Windows.tumbling(60),
        time_column="timestamp"
    )

Compact Data Format
^^^^^^^^^^^^^^^^^^^

Space-efficient format common in IoT where multiple field values are packed into arrays:

.. code-block:: python

    # Compact format - fields and values in separate arrays
    {
        "timestamp": 1234567890,
        "sensor_id": "sensor_001",
        "fields": ["temperature", "humidity", "pressure"],
        "values": [23.5, 65.2, 1013.25]
    }

    # Configure compact data handling
    from streamdaq import CompactData

    daq.configure(
        source=compact_data,
        window=Windows.tumbling(60),
        time_column="timestamp",
        compact_data=CompactData()
            .with_fields_column("fields")
            .with_values_column("values")
            .with_values_dtype(float)
    )

    # Now you can reference individual fields directly
    daq.check(dqm.mean('temperature'), must_be="(20, 30)", name="temp_check")
    daq.check(dqm.missing_count('humidity'), must_be="<5", name="humidity_missing")

Instance-Based Monitoring
-------------------------

Monitor quality metrics grouped by specific entities (users, devices, etc.):

.. code-block:: python

    # Monitor per user
    daq.configure(
        source=user_events,
        window=Windows.tumbling(3600),
        time_column="timestamp",
        instance="user_id"  # Group by user
    )

    # Monitor per device
    daq.configure(
        source=device_metrics,
        window=Windows.sliding(300, 60),
        time_column="timestamp",
        instance="device_id"  # Group by device
    )

    # Monitor per location
    daq.configure(
        source=sensor_data,
        window=Windows.tumbling(900),
        time_column="timestamp",
        instance="location"  # Group by location
    )

**Benefits of instance-based monitoring**:
- Detect issues affecting specific users/devices
- Compare quality across different entities
- Isolate problems to specific segments

Schema Validation
-----------------

Ensure data quality at the schema level using Pydantic models:

.. code-block:: python

    from pydantic import BaseModel, Field
    from streamdaq.SchemaValidator import create_schema_validator, AlertMode
    from typing import Optional

    # Define your data schema
    class SensorData(BaseModel):
        sensor_id: str = Field(..., min_length=1)
        timestamp: int = Field(..., gt=0)
        temperature: float = Field(..., ge=-50, le=100)
        humidity: Optional[float] = Field(None, ge=0, le=100)

    # Create validator
    validator = create_schema_validator(
        schema=SensorData,
        alert_mode=AlertMode.PERSISTENT,
        log_violations=True,
        deflect_violating_records=True
    )

    # Use in configuration
    daq.configure(
        source=sensor_stream,
        window=Windows.tumbling(300),
        time_column="timestamp",
        schema_validator=validator
    )

Performance Optimization
------------------------

Window Size Selection
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Small windows: More responsive, higher overhead
    window=Windows.tumbling(30)  # 30-second windows

    # Large windows: Less responsive, lower overhead
    window=Windows.tumbling(3600)  # 1-hour windows

    # Balanced approach
    window=Windows.sliding(duration=300, hop=60)  # 5-min windows, 1-min updates

Memory Management
^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Limit late data tolerance for memory efficiency
    daq.configure(
        wait_for_late=30,  # Only wait 30 seconds
        # ... other config
    )

    # Use appropriate window sizes for your data volume
    # High-volume streams: larger windows
    # Low-volume streams: smaller windows for responsiveness

Common Configuration Patterns
-----------------------------

Real-Time Monitoring
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # High-frequency, low-latency monitoring
    daq.configure(
        source=real_time_stream,
        window=Windows.sliding(duration=60, hop=10),  # 1-min windows, 10-sec updates
        time_column="timestamp",
        wait_for_late=5  # Minimal late data tolerance
    )

Batch Processing
^^^^^^^^^^^^^^^^

.. code-block:: python

    # Periodic, comprehensive analysis
    daq.configure(
        source=batch_data,
        window=Windows.tumbling(3600),  # 1-hour non-overlapping windows
        time_column="timestamp",
        wait_for_late=300  # 5-minute tolerance for completeness
    )

IoT Sensor Monitoring
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Handle compact sensor data with device grouping
    daq.configure(
        source=sensor_stream,
        window=Windows.sliding(duration=900, hop=300),  # 15-min windows, 5-min updates
        time_column="timestamp",
        instance="device_id",
        compact_data=CompactData().with_fields_column("metrics").with_values_column("readings")
    )

Troubleshooting Configuration
-----------------------------

Common Issues
^^^^^^^^^^^^^

**Issue**: "No data in windows"

**Solutions**:
- Check time column format and parsing
- Verify data source is producing data
- Ensure window size matches data frequency

**Issue**: "High memory usage"

**Solutions**:
- Reduce `wait_for_late` parameter
- Use larger window sizes
- Implement data sampling for high-volume streams

**Issue**: "Delayed results"

**Solutions**:
- Reduce window size
- Decrease `wait_for_late` tolerance
- Use sliding windows instead of tumbling

Debugging Configuration
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Test configuration with small dataset
    test_data = pw.debug.table_from_markdown("""
        | timestamp | user_id | events
      1 | 1         | user_1  | 5
      2 | 2         | user_2  | 8
    """)

    daq = StreamDaQ().configure(
        source=test_data,
        window=Windows.tumbling(2),
        time_column="timestamp"
    )
    daq.check(dqm.count('events'), name="test_count")
    daq.watch_out()

Next Steps
----------

Now that you understand configuration, continue with:

- :doc:`measures` - Learn about available quality measures
- :doc:`assessment-functions` - Define quality criteria
- :doc:`output-handling` - Route results to your systems
- :doc:`migration-guide` - Advanced multi-source patterns