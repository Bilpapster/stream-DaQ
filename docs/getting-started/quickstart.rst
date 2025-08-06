⚡ 5-Minute Quickstart
========================

Let's get Stream DaQ running with a complete example that monitors data quality in real-time. You'll have a working monitoring setup in less than 5 minutes!

Step 1: Install Stream DaQ
-----------------------------

.. code-block:: bash

    pip install streamdaq

Step 2: Create Your First Monitor
-------------------------------------

Create a new Python file called ``my_first_monitor.py`` and add this code:

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
    import pandas as pd
    from datetime import datetime, timedelta
    import time

    # Sample streaming data (simulating real-time events)
    def generate_sample_data():
        """Generate sample e-commerce events"""
        events = []
        base_time = datetime.now()
        
        for i in range(50):
            event = {
                'user_id': f'user_{i % 10}',  # 10 different users
                'event_type': 'purchase' if i % 3 == 0 else 'view',
                'amount': round(10 + (i * 1.5) % 100, 2),
                'timestamp': base_time + timedelta(seconds=i * 2),
                'session_id': f'session_{i // 5}'  # 5 events per session
            }
            events.append(event)
        
        return pd.DataFrame(events)

    # Step 1: Set up your data quality monitor
    daq = StreamDaQ().configure(
        window=Windows.tumbling(10),  # 10-second windows
        instance="user_id",          # Monitor per user
        time_column="timestamp",     # Use timestamp for windowing
        wait_for_late=2,            # Wait 2 seconds for late data
        time_format=None            # Auto-detect datetime format
    )

    # Step 2: Define what "good data quality" means for your use case
    daq.add(dqm.count('event_type'), assess=">0", name="has_events") \
       .add(dqm.distinct_count('event_type'), assess=">=1", name="event_variety") \
       .add(dqm.max('amount'), assess="<=200", name="reasonable_amounts") \
       .add(dqm.mean('amount'), assess="(10, 150)", name="avg_amount_range")

    # Step 3: Start monitoring (this will process your data stream)
    print("🚀 Starting Stream DaQ monitoring...")
    print("📊 Processing sample e-commerce events...")
    
    # Simulate streaming data
    sample_data = generate_sample_data()
    results = daq.watch_out(sample_data)
    
    print("✅ Monitoring complete! Check the results above.")

Step 3: Run Your Monitor
----------------------------

Run your Python script:

.. code-block:: bash

    python my_first_monitor.py

You should see output similar to this:

.. code-block:: text

    🚀 Starting Stream DaQ monitoring...
    📊 Processing sample e-commerce events...
    
    | user_id | window_start        | window_end          | has_events | event_variety | reasonable_amounts | avg_amount_range |
    |---------|--------------------|--------------------|------------|---------------|-------------------|------------------|
    | user_0  | 2024-01-15 10:00:00| 2024-01-15 10:00:10| (5, True)  | (2, True)     | (45.5, True)      | (32.1, True)     |
    | user_1  | 2024-01-15 10:00:00| 2024-01-15 10:00:10| (3, True)  | (1, True)     | (89.2, True)      | (65.4, True)     |
    
    ✅ Monitoring complete! Check the results above.

🎉 Congratulations!
------------------------

You just:

- ✅ **Monitored 4 different quality metrics** across streaming data
- ✅ **Got real-time results** for each user and time window  
- ✅ **Received pass/fail assessments** for each quality check
- ✅ **Handled windowing and late data** automatically

Understanding Your Results
---------------------------------

Each row represents quality metrics for one user in one time window:

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **has_events**: ``(5, True)``
        :class-header: bg-success text-white
        
        Found **5 events** in the window, passed the ">0" check ✅

    .. grid-item-card:: **event_variety**: ``(2, True)``
        :class-header: bg-success text-white
        
        Found **2 distinct** event types, passed the ">=1" check ✅

    .. grid-item-card:: **reasonable_amounts**: ``(45.5, True)``
        :class-header: bg-success text-white
        
        **Max amount** was 45.5, passed the "<=200" check ✅

    .. grid-item-card:: **avg_amount_range**: ``(32.1, True)``
        :class-header: bg-success text-white
        
        **Average amount** was 32.1, passed the "(10, 150)" range check ✅

What Just Happened?
-------------------------

1. **Data Streaming**: Stream DaQ processed your data as if it were coming from a real-time stream
2. **Windowing**: Data was grouped into 10-second tumbling windows per user
3. **Quality Assessment**: Each window was checked against your 4 quality rules
4. **Real-time Results**: You got immediate feedback on data quality as a stream of results

Next Steps
-------------

Now that you've seen Stream DaQ in action:

- 📚 **Learn the concepts**: :doc:`../concepts/index` - Understand windows, measures, and assessments
- 🔍 **Go deeper**: :doc:`first-monitoring` - Build a monitoring setup step-by-step
- 💡 **See more examples**: :doc:`../examples/index` - Explore real-world use cases
- ⚙️ **Advanced config**: :doc:`../user-guide/index` - Master all configuration options

.. admonition:: Try This Next
   :class: tip

   Modify the assessment criteria in your code:
   
   - Change ``assess=">0"`` to ``assess=">3"`` and see what happens
   - Try ``assess="==2"`` for event variety
   - Experiment with different window sizes using ``Windows.tumbling(5)``