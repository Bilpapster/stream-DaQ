🥷 Real-world Cases
====================

Practical examples from production environments.

IoT Sensor Network
------------------

**Scenario**: Manufacturing plant with 500+ temperature sensors

.. code-block:: python

    # Monitor sensor health and data quality
    daq = StreamDaQ().configure(
        source=sensor_stream,
        window=Windows.sliding(300, 60),  # 5-min windows, 1-min updates
        time_column="timestamp",
        instance="sensor_id"
    )
    
    # Volume check - each sensor should report every 30 seconds
    daq.check(dqm.count('readings'), must_be=">8", name="sensor_alive")
    
    # Range validation - temperature should be reasonable
    daq.check(dqm.mean('temperature'), must_be="(15, 45)", name="temp_range")
    
    # Stability check - temperature shouldn't fluctuate wildly
    daq.check(dqm.std('temperature'), must_be="<5", name="temp_stable")

E-commerce Transaction Monitoring
---------------------------------

**Scenario**: Online store processing thousands of orders per hour

.. code-block:: python

    # Monitor transaction quality and business metrics
    daq = StreamDaQ().configure(
        source=transaction_stream,
        window=Windows.tumbling(3600),  # Hourly reports
        time_column="order_time"
    )
    
    # Business volume check
    daq.check(dqm.count('orders'), must_be=">100", name="hourly_volume")
    
    # Revenue validation
    daq.check(dqm.sum('order_value'), must_be=">5000", name="hourly_revenue")
    
    # Data completeness
    daq.check(dqm.missing_count('customer_email'), must_be="==0", name="email_required")

Financial Trading System
------------------------

**Scenario**: High-frequency trading with strict latency requirements

.. code-block:: python

    # Monitor trade execution quality
    daq = StreamDaQ().configure(
        source=trade_stream,
        window=Windows.sliding(60, 10),  # 1-min windows, 10-sec updates
        time_column="execution_time",
        wait_for_late=0  # No tolerance for late trades
    )
    
    # Latency monitoring
    daq.check(dqm.percentile('latency_ms', 95), must_be="<50", name="p95_latency")
    
    # Error rate monitoring
    error_rate = dqm.count('failed_trades') / dqm.count('total_trades') * 100
    daq.check(error_rate, must_be="<0.1", name="error_rate")

Web Analytics Pipeline
-----------------------

**Scenario**: Real-time user behavior tracking

.. code-block:: python

    # Monitor user engagement quality
    daq = StreamDaQ().configure(
        source=clickstream,
        window=Windows.tumbling(900),  # 15-minute windows
        time_column="event_timestamp",
        instance="user_session"
    )
    
    # Session activity
    daq.check(dqm.distinct_count('page_view'), must_be=">1", name="active_session")
    
    # Data format validation
    url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    daq.check(dqm.pattern_conformance_fraction('url', url_pattern), 
              must_be=">0.95", name="valid_urls")

Key Patterns
-------------

**Volume Monitoring**: Always check if data is arriving at expected rates
**Range Validation**: Ensure values fall within business-logical bounds  
**Format Compliance**: Validate data structure and patterns
**Temporal Consistency**: Monitor for gaps or delays in data arrival
**Cross-Field Validation**: Check relationships between different fields

Next Steps
----------

- :doc:`../user-guide/index` - Detailed configuration guides
- :doc:`advanced-examples` - Complex multi-source scenarios
- :doc:`../concepts/index` - Understand the theory behind these patterns
