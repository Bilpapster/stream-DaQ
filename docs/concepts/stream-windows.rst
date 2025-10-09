🪟 Stream Windows
=================

Imagine trying to calculate the average temperature from a thermometer that never stops reading. How do you compute "average" from an infinite stream of data? The answer is **windows** – time boundaries that slice infinite streams into manageable, finite chunks.

Windows are the fundamental abstraction that makes stream processing possible. Without them, questions like *"How many users visited our site this hour?"* or *"What's the average response time this minute?"* would be impossible to answer.

.. admonition:: Why windows matter
   :class: tip

   In batch processing, you work with complete datasets. In stream processing, you work with **windows of an infinite dataset**. Getting windowing right is crucial for meaningful stream analytics and quality monitoring.

Understanding Time in Data Streams
-----------------------------------

Before diving into window types, we need to understand a fundamental concept: **there are actually two different "times" in streaming data**.

### Event Time vs System Time

.. grid:: 1 1 2 2
    :gutter: 4

    .. grid-item-card:: 🕐 **Event Time**
        :class-header: bg-primary text-white

        **When the event actually happened** in the real world. This timestamp is embedded in your data.

    .. grid-item-card:: 🖥️ **System Time**
        :class-header: bg-info text-white

        **When your system processed the event**. This is when Stream DaQ sees the data.

**Real-World Example: IoT Temperature Sensors**

.. code-block:: python

    # Your data might look like this:
    sensor_reading = {
        'sensor_id': 'temp_01',
        'temperature': 23.5,
        'event_time': '2024-01-15 14:30:00',    # When sensor took the reading
        'system_time': '2024-01-15 14:30:03'    # When data reached your system
    }

The **3-second difference** between event time and system time could be due to:
- Network latency
- Sensor buffering
- Processing delays
- System clock differences

### Why This Matters for Windows

Consider this scenario:

.. code-block:: text

    14:30:00 → Sensor takes reading (event_time)
    14:30:03 → Data arrives at Stream DaQ (system_time)

    Question: Which 1-minute window does this belong to?
    - 14:30:00-14:31:00 (based on when it happened)
    - 14:30:03-14:31:03 (based on when we saw it)

**Stream DaQ uses event time by default** because we care about when things actually happened, not when we happened to process them.

.. admonition:: Flexible Time Assignment
   :class: note

   While most systems force you to use a specific time field, **Stream DaQ lets you choose any timestamp column** for window assignment. This flexibility is crucial when dealing with complex data pipelines.

Types of Windows
----------------

Stream DaQ supports three types of windows, each designed for different use cases:

### 1. Time-Based Windows

Time-based windows group data by time intervals. There are two variants:

#### Tumbling Windows (Non-Overlapping)

.. raw:: html

    <div style="text-align: center; margin: 2em 0;">
        <pre style="background: #f6f8fa; padding: 1em; border-radius: 4px; font-family: monospace; display: inline-block;">
    Time:  10:00    10:05    10:10    10:15    10:20
           |--------|--------|--------|--------|
           Window 1 Window 2 Window 3 Window 4

           Each event belongs to exactly ONE window
        </pre>
    </div>

**Use Case: Hourly Sales Reports**

.. code-block:: python

    from streamdaq import StreamDaQ, Windows

    # Monitor sales every hour (no overlap)
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3600),  # 3600 seconds = 1 hour
        time_column="transaction_time"
    )

Perfect for: Financial reporting, batch job monitoring, periodic health checks

#### Sliding Windows (Overlapping)

.. raw:: html

    <div style="text-align: center; margin: 2em 0;">
        <pre style="background: #f6f8fa; padding: 1em; border-radius: 4px; font-family: monospace; display: inline-block;">
    Time:  10:00    10:02    10:04    10:06    10:08
           |--------|
                |--------|
                     |--------|
                          |--------|
           5-minute windows, starting every 2 minutes

           Events can belong to MULTIPLE windows
        </pre>
    </div>

**Use Case: Real-Time Anomaly Detection**

.. code-block:: python

    # Monitor API response times with sliding windows
    # 5-minute windows, updated every 1 minute
    daq = StreamDaQ().configure(
        window=Windows.sliding(300, 60),  # window_size=300s, slide=60s
        time_column="request_timestamp"
    )

Perfect for: Real-time dashboards, anomaly detection, trend analysis

### 2. Session-Based Windows

Session windows group events by **continuous activity**, separated by periods of inactivity.

.. raw:: html

    <div style="text-align: center; margin: 2em 0;">
        <pre style="background: #f6f8fa; padding: 1em; border-radius: 4px; font-family: monospace; display: inline-block;">
    User Activity Timeline:

    |●●●●●------●●●●●●●●●------●●●●|
     Session 1    Session 2     Session 3
     (4 events)   (8 events)    (4 events)

    Gap > 30 seconds = new session starts
        </pre>
    </div>

Session windows are defined as **continuous activity within a time frame, separated by a gap of inactivity** of specific time duration. They're perfect for analyzing user actions like clickstream data.

**Use Case: Website User Sessions**

.. code-block:: python

    # Track user behavior sessions
    # New session if >30 seconds gap between clicks
    daq = StreamDaQ().configure(
        window=Windows.session(30),  # 30-second timeout
        instance="user_id",         # Separate sessions per user
        time_column="click_time"
    )

**Real-World Example: E-commerce Analytics**

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

    # Monitor user session quality
    session_monitor = StreamDaQ().configure(
        window=Windows.session(1800),  # 30-minute timeout
        instance="user_id",
        time_column="event_timestamp"
    )

    session_monitor.add(dqm.count('page_views'), assess=">0", name="active_session") \
                   .add(dqm.distinct_count('page_category'), assess=">1", name="diverse_browsing") \
                   .add(dqm.session_duration(), assess="(60, 3600)", name="reasonable_duration")

Perfect for: User behavior analysis, fraud detection, application monitoring

### 3. Count-Based Windows (Workaround Available)

Count-based windows group data by the **number of events** rather than time.

.. raw:: html

    <div style="text-align: center; margin: 2em 0;">
        <pre style="background: #f6f8fa; padding: 1em; border-radius: 4px; font-family: monospace; display: inline-block;">
    Events: ●●●●●|●●●●●|●●●●●|●●●●●
            Win 1  Win 2  Win 3  Win 4

            Every 5 events = 1 window
        </pre>
    </div>

.. admonition:: Current Limitation & Workaround
   :class: warning

   **Stream DaQ doesn't natively support count-based windows yet** because current Python streaming frameworks lack this functionality. We're eager to add support as soon as any Python-based stream processing framework implements this feature, but it's not in our hands and we unfortunately cannot provide a timeline.

**Workaround: Synthetic Time Column**

.. code-block:: python

    import pandas as pd
    from datetime import datetime, timedelta

    def add_count_based_time(df, events_per_window=100):
        """
        Create synthetic time column for count-based windowing
        WARNING: Only works with ordered data (no late arrivals)
        """
        df = df.copy()
        df['window_number'] = df.index // events_per_window

        # Create synthetic timestamps
        base_time = datetime.now()
        df['synthetic_time'] = df['window_number'].apply(
            lambda x: base_time + timedelta(minutes=x)
        )

        return df

    # Use synthetic time for "count-based" windows
    processed_data = add_count_based_time(your_data, events_per_window=50)

    daq = StreamDaQ().configure(
        window=Windows.tumbling(60),  # 1 minute = 1 synthetic window
        time_column="synthetic_time"  # Use our synthetic timestamp
    )

This workaround simulates count-based windows, provided that you can ensure there are no out-of-order data.

Handling Late and Out-of-Order Data
-----------------------------------

Real-world data streams are messy. Events don't always arrive in the order they occurred, and some arrive fashionably late to the party.

### The Late Data Problem

.. code-block:: text

    Expected order:  Event A (10:00) → Event B (10:01) → Event C (10:02)
    Actual arrival:  Event B (10:01) → Event C (10:02) → Event A (10:00) ← LATE!

**When Event A finally arrives, what should we do?**

Sometimes, a late event is no longer relevant so we can discard it. In other cases, we want to keep it, but this may re-fire all computations again if the window computations have already been completed.

### Stream DaQ's Flexible Cut-off Mechanism

To support all cases and domains, **Stream DaQ enables a flexible cut-off mechanism** which specifies the maximum amount of time we can wait for late events.

.. code-block:: python

    daq = StreamDaQ().configure(
        window=Windows.tumbling(60),
        time_column="event_time",
        wait_for_late=2  # Wait up to 2 seconds for late events
    )

**How it works:**

If the cut-off is set to 2 seconds, then any element that arrives more than two seconds **after** its window has closed is discarded.

.. raw:: html

    <div style="text-align: center; margin: 2em 0;">
        <pre style="background: #f6f8fa; padding: 1em; border-radius: 4px; font-family: monospace; display: inline-block;">
    Window: 10:00:00 - 10:01:00
    Window closes: 10:01:00
    Cut-off time: 10:01:02 (window close + wait_for_late)

    Event arrives 10:01:01 → ✅ Accepted (within cut-off)
    Event arrives 10:01:03 → ❌ Discarded (too late)
        </pre>
    </div>

### Choosing the Right Cut-off Strategy

Different use cases require different approaches to late data:

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Financial Trading**
        :class-header: bg-danger text-white

        ``wait_for_late=0`` - No tolerance for late data. Speed is critical.

    .. grid-item-card:: **IoT Monitoring**
        :class-header: bg-warning text-dark

        ``wait_for_late=30`` - Moderate tolerance. Network issues are common.

    .. grid-item-card:: **User Analytics**
        :class-header: bg-success text-white

        ``wait_for_late=300`` - High tolerance. User experience matters more than speed.

    .. grid-item-card:: **Batch Integration**
        :class-header: bg-info text-white

        ``wait_for_late=3600`` - Very high tolerance. Accuracy over speed.

**Real-World Example

**Real-World Example: IoT Sensor Monitoring**

.. code-block:: python

    # Monitoring temperature sensors with network reliability issues
    daq = StreamDaQ().configure(
        window=Windows.tumbling(300),   # 5-minute windows
        time_column="sensor_timestamp", # Use when sensor took reading
        wait_for_late=60,              # Wait 1 minute for late sensors
        instance="sensor_id"           # Monitor each sensor separately
    )

    # This configuration handles:
    # ✅ Network hiccups causing 30-second delays
    # ✅ Sensor clock synchronization issues
    # ✅ Temporary connectivity problems
    # ❌ Sensors that are offline for >1 minute (discarded)

Choosing the Right Window Type
------------------------------

Different analysis needs require different windowing strategies:

### Time-Based Windows: When to Use What

**Tumbling Windows** are perfect for:

.. code-block:: python

    # ✅ Periodic reporting (hourly, daily, monthly)
    Windows.tumbling(3600)  # Hourly sales reports

    # ✅ Resource utilization monitoring
    Windows.tumbling(60)    # CPU usage per minute

    # ✅ Compliance reporting
    Windows.tumbling(86400) # Daily data quality reports

**Sliding Windows** excel at:

.. code-block:: python

    # ✅ Real-time anomaly detection
    Windows.sliding(300, 60)  # 5-min window, updated every minute

    # ✅ Trend analysis
    Windows.sliding(3600, 300) # 1-hour trends, updated every 5 minutes

    # ✅ Real-time dashboards
    Windows.sliding(600, 30)   # 10-min metrics, updated every 30 seconds

### Session Windows: Behavioral Analysis

Session windows are ideal when you need to understand **user journeys** or **process flows**:

.. code-block:: python

    # Manufacturing: Track production runs
    production_sessions = StreamDaQ().configure(
        window=Windows.session(600),   # 10-minute idle timeout
        instance="machine_id",
        time_column="operation_timestamp"
    )

    # Monitor production session quality
    production_sessions.add(dqm.count('operations'), assess=">10", name="productive_session") \
                       .add(dqm.distinct_count('operation_type'), assess=">2", name="diverse_operations") \
                       .add(dqm.session_duration(), assess="(300, 7200)", name="reasonable_duration")

**Real-World Session Examples:**

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Web Analytics**
        :class-header: bg-primary text-white

        User browsing sessions with 30-minute timeout

    .. grid-item-card:: **Manufacturing**
        :class-header: bg-success text-white

        Production runs with 10-minute idle timeout

    .. grid-item-card:: **Gaming**
        :class-header: bg-info text-white

        Game sessions with 5-minute AFK timeout

    .. grid-item-card:: **Call Centers**
        :class-header: bg-warning text-dark

        Customer interaction sessions with 2-minute silence timeout

Advanced Window Configuration
-----------------------------

Stream DaQ provides fine-grained control over windowing behavior:

.. code-block:: python

    from streamdaq import StreamDaQ, Windows

    # Production-grade window configuration
    daq = StreamDaQ().configure(
        # Window definition
        window=Windows.tumbling(300),           # 5-minute windows

        # Time handling
        time_column="event_timestamp",          # Custom time field
        time_format="%Y-%m-%d %H:%M:%S",       # Specify format if needed
        wait_for_late=30,                      # 30-second grace period

        # Data organization
        instance="customer_id"                 # Separate windows per customer
    )

Common Pitfalls and Best Practices
----------------------------------

.. admonition:: ⚠️ Common Mistakes
   :class: warning

   - **Wrong time column**: Using system time instead of event time leads to incorrect windows
   - **Too small grace period**: Discarding too much valid late data
   - **Too large windows**: Running out of memory on high-volume streams
   - **Ignoring time zones**: Not accounting for timezone differences in global systems

.. admonition:: ✅ Best Practices
   :class: tip

   - **Start simple**: Begin with tumbling windows, add complexity as needed
   - **Monitor late data**: Track how much data arrives late to tune ``wait_for_late``
   - **Test with real data**: Synthetic data doesn't show real timing issues
   - **Plan for scale**: Consider memory and processing requirements early

What's Next?
------------

Now that you understand how to slice infinite streams into manageable windows:

- 📊 **Understand data formats**: :doc:`compact-vs-native-data` - How different data formats work seamlessly with windows
- 📏 **Learn about measures**: :doc:`measures-and-assessments` - What to calculate within each window
- ⚡ **Explore real-time concepts**: :doc:`real-time-monitoring` - Production considerations for windowed monitoring
- 💡 **See windowing in action**: :doc:`../examples/index` - Real-world windowing patterns

|made_with_love|