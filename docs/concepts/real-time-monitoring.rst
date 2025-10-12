⏱️ Real-time Monitoring
=============================

Real-time data quality monitoring presents unique challenges that don't exist in batch processing. Stream DaQ handles these complexities automatically while giving you control over the trade-offs.

Key Challenges
--------------

**Late Arrivals**
   Data doesn't always arrive in perfect order. Network delays, processing bottlenecks, and system failures can cause events to arrive late.

**Processing Time vs Event Time**
   The time when data is processed differs from when events actually occurred. Stream DaQ uses event time for accurate windowing.

**Watermarks**
   Mechanisms to handle late data by defining how long to wait for delayed events before finalizing window results.

**Backpressure**
   When data arrives faster than it can be processed, systems need strategies to handle the overflow.

Stream DaQ's Approach
---------------------

**Automatic Late Data Handling**

.. code-block:: python

    # Configure tolerance for late arrivals
    daq.configure(
        wait_for_late=30,  # Wait 30 seconds for late data
        time_column="event_timestamp"  # Use event time, not processing time
    )

**Flexible Watermarking**

Stream DaQ automatically manages watermarks based on your ``wait_for_late`` setting. You don't need to configure complex watermark strategies.

**Built-in Backpressure Management**

The system automatically handles varying data rates without manual intervention.

Trade-offs to Consider
------------------------

**Latency vs Completeness**
   - Lower ``wait_for_late`` = faster results, might miss late data
   - Higher ``wait_for_late`` = more complete results, slower processing

**Memory vs Accuracy**
   - Larger windows = more memory usage but better statistical accuracy
   - Smaller windows = less memory but potentially noisier results

Best Practices
--------------

1. **Start with reasonable defaults**: ``wait_for_late=30`` works for most use cases
2. **Monitor your data patterns**: Understand typical arrival delays in your system
3. **Use appropriate window sizes**: Match window duration to your data frequency
4. **Test with realistic data**: Validate behavior under actual load conditions

Next Steps
----------

- :doc:`stream-windows` - Learn how windowing makes real-time processing manageable
- :doc:`measures-and-assessments` - Understand the building blocks of quality checks
- :doc:`../examples/index` - See real-time monitoring in action