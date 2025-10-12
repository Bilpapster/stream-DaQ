📏 Measures
===========

Data quality measures are the heart of Stream DaQ monitoring. This guide covers all available measures, when to use them, and how to combine them effectively for comprehensive quality monitoring.

.. contents:: In this guide:
   :local:
   :depth: 2

Overview
--------

Stream DaQ provides a comprehensive set of built-in measures through the ``DaQMeasures`` (``dqm``) module. Each measure calculates a specific aspect of data quality within your configured time windows.

.. code-block:: python

    from streamdaq import DaQMeasures as dqm

    # Basic usage pattern
    daq.check(dqm.measure_name('column'), must_be="criteria", name="check_name")

Basic Statistical Measures
--------------------------

These measures provide fundamental statistical insights about your data.

Count Measures
^^^^^^^^^^^^^^

**Purpose**: Monitor data volume and availability

.. code-block:: python

    # Total record count
    daq.check(dqm.count('events'), must_be=">100", name="volume_check")

    # Count of non-null values
    daq.check(dqm.count('temperature'), must_be=">50", name="temp_availability")

    # Distinct value count
    daq.check(dqm.distinct_count('user_id'), must_be=">10", name="user_diversity")

**Use cases**:
- Detect data pipeline failures (low counts)
- Monitor user engagement (distinct users)
- Validate data completeness

Central Tendency Measures
^^^^^^^^^^^^^^^^^^^^^^^^^

**Purpose**: Understand typical values and detect shifts

.. code-block:: python

    # Mean (average)
    daq.check(dqm.mean('response_time'), must_be="<500", name="avg_response")

    # Median (50th percentile)
    daq.check(dqm.median('transaction_amount'), must_be="(10, 1000)", name="typical_amount")

    # Mode (most frequent value)
    daq.check(dqm.mode('status_code'), must_be="==200", name="common_status")

**Use cases**:
- Performance monitoring (response times)
- Business metrics (transaction amounts)
- System health (error rates)

Variability Measures
^^^^^^^^^^^^^^^^^^^^

**Purpose**: Monitor data consistency and detect anomalies

.. code-block:: python

    # Standard deviation
    daq.check(dqm.std('cpu_usage'), must_be="<20", name="cpu_stability")

    # Variance
    daq.check(dqm.var('network_latency'), must_be="<100", name="latency_variance")

    # Range (max - min)
    daq.check(dqm.range('temperature'), must_be="<30", name="temp_range")

**Use cases**:
- System stability monitoring
- Quality control in manufacturing
- Network performance analysis

Extreme Value Measures
^^^^^^^^^^^^^^^^^^^^^^

**Purpose**: Monitor boundaries and detect outliers

.. code-block:: python

    # Minimum value
    daq.check(dqm.min('disk_space'), must_be=">1000", name="min_disk_space")

    # Maximum value
    daq.check(dqm.max('memory_usage'), must_be="<90", name="max_memory")

    # Percentiles
    daq.check(dqm.percentile('response_time', 95), must_be="<2000", name="p95_response")
    daq.check(dqm.percentile('error_rate', 99), must_be="<5", name="p99_errors")

**Use cases**:
- SLA monitoring (95th percentile response times)
- Resource utilization limits
- Outlier detection

Data Quality Specific Measures
------------------------------

These measures are specifically designed for data quality assessment.

Missing Data Detection
^^^^^^^^^^^^^^^^^^^^^^

**Purpose**: Monitor data completeness and identify gaps

.. code-block:: python

    # Count of missing (null) values
    daq.check(dqm.missing_count('email'), must_be="<10", name="missing_emails")

    # Percentage of missing values
    daq.check(dqm.missing_percentage('phone'), must_be="<5", name="missing_phone_pct")

    # Check if any values are missing
    daq.check(dqm.has_missing('critical_field'), must_be="==False", name="no_missing_critical")

**Use cases**:
- Data pipeline validation
- Form completion monitoring
- Critical field availability

Duplicate Detection
^^^^^^^^^^^^^^^^^^^

**Purpose**: Identify data duplication issues

.. code-block:: python

    # Count of duplicate values
    daq.check(dqm.duplicate_count('transaction_id'), must_be="==0", name="no_duplicate_txns")

    # Percentage of duplicates
    daq.check(dqm.duplicate_percentage('user_session'), must_be="<1", name="session_duplicates")

**Use cases**:
- Transaction integrity
- Data deduplication validation
- ETL process monitoring

Range Conformance
^^^^^^^^^^^^^^^^^

**Purpose**: Monitor adherence to expected value ranges

.. code-block:: python

    # Fraction of values within range
    daq.check(dqm.range_conformance_fraction('age', 0, 120), must_be=">0.95", name="valid_ages")

    # Count of values outside range
    daq.check(dqm.out_of_range_count('temperature', -10, 50), must_be="<5", name="temp_outliers")

**Use cases**:
- Sensor data validation
- Business rule compliance
- Data entry quality

Pattern Conformance
^^^^^^^^^^^^^^^^^^^

**Purpose**: Validate data format and structure

.. code-block:: python

    # Regex pattern matching
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
    daq.check(dqm.pattern_conformance_fraction('email', email_pattern), must_be=">0.9", name="valid_emails")

    # Phone number format
    phone_pattern = r'^\\+?1?[2-9]\\d{2}[2-9]\\d{2}\\d{4}$'
    daq.check(dqm.pattern_conformance_count('phone', phone_pattern), must_be=">100", name="valid_phones")

**Use cases**:
- Email validation
- Phone number formatting
- ID format compliance

Advanced Measures
-----------------

Sophisticated measures for complex quality scenarios.

Trend Analysis
^^^^^^^^^^^^^^

**Purpose**: Detect changes and patterns over time

.. code-block:: python

    # Linear trend slope
    daq.check(dqm.trend('cpu_usage', 'timestamp'), must_be="<0.1", name="cpu_trend")

    # Custom trend functions
    def is_stable_trend(slope):
        return abs(slope) < 0.05

    daq.check(dqm.trend('memory', 'timestamp'), must_be=is_stable_trend, name="memory_stability")

**Use cases**:
- Performance degradation detection
- Capacity planning
- System health monitoring

Frozen Data Detection
^^^^^^^^^^^^^^^^^^^^^

**Purpose**: Identify stale or unchanging data

.. code-block:: python

    # Detect if values haven't changed
    daq.check(dqm.is_frozen('sensor_reading'), must_be="==False", name="sensor_active")

    # Count of frozen periods
    daq.check(dqm.frozen_periods('heartbeat'), must_be="==0", name="no_frozen_heartbeat")

**Use cases**:
- Sensor malfunction detection
- Data pipeline health
- System responsiveness

Combining Measures
------------------

Create comprehensive quality checks by combining multiple measures.

Arithmetic Operations
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Sum of multiple measures
    total_missing = dqm.missing_count('field1') + dqm.missing_count('field2') + dqm.missing_count('field3')
    daq.check(total_missing, must_be="<10", name="total_missing_fields")

    # Ratio calculations
    error_rate = dqm.count('errors') / dqm.count('total_requests') * 100
    daq.check(error_rate, must_be="<5", name="error_rate_percent")

    # Complex calculations
    data_quality_score = (dqm.count('valid_records') / dqm.count('total_records')) * 100
    daq.check(data_quality_score, must_be=">95", name="quality_score")

Custom Quality Functions
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Custom quality functions
    def comprehensive_health_check(count, missing, duplicates):
        if count < 100:
            return False  # Insufficient data
        if missing > count * 0.1:
            return False  # Too many missing values
        if duplicates > 0:
            return False  # Any duplicates are bad
        return True

    # Apply custom logic
    import pathway as pw
    health_check = pw.apply_with_type(
        comprehensive_health_check, 
        bool,
        dqm.count('records'),
        dqm.missing_count('critical_field'),
        dqm.duplicate_count('id')
    )
    daq.check(health_check, must_be="==True", name="overall_health")

Measure Selection Guide
-----------------------

By Data Type
^^^^^^^^^^^^

**Numerical Data**:
- Volume: ``count()``, ``distinct_count()``
- Central tendency: ``mean()``, ``median()``
- Variability: ``std()``, ``range()``
- Extremes: ``min()``, ``max()``, ``percentile()``

**Categorical Data**:
- Diversity: ``distinct_count()``, ``mode()``
- Completeness: ``missing_count()``, ``missing_percentage()``
- Patterns: ``pattern_conformance_fraction()``

**Time Series Data**:
- Trends: ``trend()``
- Stability: ``is_frozen()``, ``std()``
- Continuity: ``missing_count()``

By Use Case
^^^^^^^^^^^

**System Monitoring**:
.. code-block:: python

    # CPU monitoring
    daq.check(dqm.mean('cpu_percent'), must_be="<80", name="avg_cpu")
    daq.check(dqm.max('cpu_percent'), must_be="<95", name="peak_cpu")
    daq.check(dqm.trend('cpu_percent', 'timestamp'), must_be="<0.1", name="cpu_trend")

**Business Metrics**:
.. code-block:: python

    # Revenue monitoring
    daq.check(dqm.sum('revenue'), must_be=">10000", name="daily_revenue")
    daq.check(dqm.count('transactions'), must_be=">500", name="transaction_volume")
    daq.check(dqm.mean('order_value'), must_be="(20, 200)", name="avg_order_value")

**Data Pipeline Validation**:
.. code-block:: python

    # Completeness checks
    daq.check(dqm.missing_count('user_id'), must_be="==0", name="no_missing_users")
    daq.check(dqm.duplicate_count('transaction_id'), must_be="==0", name="unique_transactions")
    daq.check(dqm.count('records'), must_be=">1000", name="sufficient_data")

Performance Considerations
--------------------------

Efficient Measure Usage
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Efficient: Reuse calculations
    record_count = dqm.count('records')
    daq.check(record_count, must_be=">100", name="volume")
    daq.check(dqm.missing_count('field') / record_count, must_be="<0.1", name="missing_rate")

    # Less efficient: Recalculate
    daq.check(dqm.count('records'), must_be=">100", name="volume")
    daq.check(dqm.missing_count('field') / dqm.count('records'), must_be="<0.1", name="missing_rate")

Custom Measures
---------------

Create your own measures for specific business logic:

.. code-block:: python

    # Define custom measure function
    def business_health_score(revenue, users, errors):
        if errors > users * 0.01:  # Error rate > 1%
            return 0
        if revenue < 1000:  # Minimum revenue threshold
            return 0
        return min(100, (revenue / 10000) * 100)  # Scale to 0-100

    # Apply as measure
    health_score = pw.apply_with_type(
        business_health_score,
        float,
        dqm.sum('revenue'),
        dqm.distinct_count('user_id'),
        dqm.count('errors')
    )
    
    daq.check(health_score, must_be=">70", name="business_health")

Next Steps
----------

Now that you understand measures, continue with:

- :doc:`assessment-functions` - Define quality criteria for your measures
- :doc:`output-handling` - Route measure results to your systems
- :doc:`configuration` - Advanced configuration patterns

**Complete Measures Reference**: For a complete list of all available measures with detailed parameters, see the `DaQMeasures API documentation <https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py>`_.