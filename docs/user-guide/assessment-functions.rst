🎯 Assessment Functions
=======================

Assessment functions define the quality criteria for your measures - they determine whether your data quality is acceptable or not. This guide covers all assessment patterns, from simple thresholds to complex custom logic.

.. contents:: In this guide:
   :local:
   :depth: 2

Overview
--------

Every quality check in Stream DaQ follows this pattern:

.. code-block:: python

    daq.check(measure, must_be="criteria", name="check_name")

The ``must_be`` parameter defines your quality criteria. Stream DaQ supports multiple assessment approaches:

- **String expressions**: Simple, readable criteria like ``">100"`` or ``"(10, 50)"``
- **Custom functions**: Complex logic for sophisticated quality rules
- **Boolean expressions**: Direct true/false evaluations

String-Based Assessments
------------------------

String assessments provide an intuitive way to define common quality criteria.

Comparison Operators
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Greater than
    daq.check(dqm.count('events'), must_be=">100", name="sufficient_volume")

    # Less than
    daq.check(dqm.mean('response_time'), must_be="<500", name="fast_response")

    # Greater than or equal
    daq.check(dqm.min('temperature'), must_be=">=0", name="above_freezing")

    # Less than or equal
    daq.check(dqm.max('cpu_usage'), must_be="<=90", name="cpu_limit")

    # Equal to
    daq.check(dqm.mode('status'), must_be="==200", name="success_status")

    # Not equal to
    daq.check(dqm.missing_count('user_id'), must_be="!=0", name="no_missing_users")

Range Assessments
^^^^^^^^^^^^^^^^^

**Inclusive Ranges** (both endpoints included):

.. code-block:: python

    # Closed interval [min, max]
    daq.check(dqm.mean('temperature'), must_be="[20, 25]", name="comfort_temp")

    # Values between 20 and 25 (inclusive) are acceptable

**Exclusive Ranges** (endpoints excluded):

.. code-block:: python

    # Open interval (min, max)
    daq.check(dqm.std('latency'), must_be="(0, 100)", name="stable_latency")

    # Values between 0 and 100 (exclusive) are acceptable

**Mixed Ranges**:

.. code-block:: python

    # Half-open intervals
    daq.check(dqm.percentile('response_time', 95), must_be="(0, 2000]", name="p95_response")
    daq.check(dqm.error_rate(), must_be="[0, 5)", name="low_error_rate")

Set Membership
^^^^^^^^^^^^^^

.. code-block:: python

    # Value must be in set
    daq.check(dqm.mode('status_code'), must_be="in [200, 201, 202]", name="success_codes")

    # Value must not be in set
    daq.check(dqm.mode('error_type'), must_be="not in ['CRITICAL', 'FATAL']", name="no_critical_errors")

Practical Examples
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # System monitoring
    daq.check(dqm.mean('cpu_percent'), must_be="<80", name="cpu_normal")
    daq.check(dqm.max('memory_usage'), must_be="<=90", name="memory_limit")
    daq.check(dqm.min('disk_free'), must_be=">1000", name="disk_space")

    # Business metrics
    daq.check(dqm.sum('daily_revenue'), must_be=">10000", name="revenue_target")
    daq.check(dqm.count('new_users'), must_be="[50, 500]", name="growth_range")
    daq.check(dqm.mean('session_duration'), must_be="(60, 1800)", name="engagement_time")

    # Data quality
    daq.check(dqm.missing_percentage('email'), must_be="<5", name="email_completeness")
    daq.check(dqm.duplicate_count('transaction_id'), must_be="==0", name="unique_transactions")

Custom Assessment Functions
---------------------------

For complex quality logic, define custom assessment functions.

Simple Custom Functions
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Define assessment function
    def is_healthy_response_time(avg_time):
        """Response time is healthy if under 500ms."""
        return avg_time < 500

    # Use in quality check
    daq.check(dqm.mean('response_time'), must_be=is_healthy_response_time, name="response_health")

Multi-Criteria Functions
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    def is_system_stable(cpu_avg, cpu_max, memory_avg):
        """System is stable if all metrics are within acceptable ranges."""
        if cpu_avg > 70:  # Average CPU too high
            return False
        if cpu_max > 95:  # Peak CPU too high
            return False
        if memory_avg > 80:  # Memory usage too high
            return False
        return True

    # Apply to combined measures
    import pathway as pw
    system_health = pw.apply_with_type(
        is_system_stable,
        bool,
        dqm.mean('cpu_percent'),
        dqm.max('cpu_percent'),
        dqm.mean('memory_percent')
    )
    daq.check(system_health, must_be="==True", name="system_stability")

Business Logic Functions
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    def is_revenue_healthy(revenue, transactions, avg_order):
        """Revenue is healthy based on multiple business criteria."""
        # Minimum revenue threshold
        if revenue < 5000:
            return False
        
        # Minimum transaction volume
        if transactions < 100:
            return False
        
        # Average order value should be reasonable
        if avg_order < 10 or avg_order > 1000:
            return False
        
        # Revenue per transaction should be consistent
        calculated_avg = revenue / transactions if transactions > 0 else 0
        if abs(calculated_avg - avg_order) > avg_order * 0.1:  # 10% tolerance
            return False
        
        return True

    # Apply business logic
    revenue_health = pw.apply_with_type(
        is_revenue_healthy,
        bool,
        dqm.sum('revenue'),
        dqm.count('transactions'),
        dqm.mean('order_value')
    )
    daq.check(revenue_health, must_be="==True", name="revenue_health")

Advanced Assessment Patterns
----------------------------

Conditional Assessments
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    def adaptive_error_threshold(error_count, total_requests):
        """Error threshold adapts based on traffic volume."""
        if total_requests < 100:
            # Low traffic: no errors allowed
            return error_count == 0
        elif total_requests < 1000:
            # Medium traffic: <1% error rate
            return error_count / total_requests < 0.01
        else:
            # High traffic: <0.5% error rate
            return error_count / total_requests < 0.005

    error_assessment = pw.apply_with_type(
        adaptive_error_threshold,
        bool,
        dqm.count('errors'),
        dqm.count('requests')
    )
    daq.check(error_assessment, must_be="==True", name="adaptive_error_rate")

Trend-Based Assessments
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    def is_stable_trend(slope, current_value):
        """Trend is stable if slope is small and value is reasonable."""
        # Slope should be nearly flat
        if abs(slope) > 0.1:
            return False
        
        # Current value should be in acceptable range
        if current_value < 10 or current_value > 90:
            return False
        
        return True

    trend_stability = pw.apply_with_type(
        is_stable_trend,
        bool,
        dqm.trend('cpu_usage', 'timestamp'),
        dqm.mean('cpu_usage')
    )
    daq.check(trend_stability, must_be="==True", name="cpu_trend_stable")

Assessment Best Practices
-------------------------

Clear and Descriptive Names
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Good: Descriptive names
    daq.check(dqm.count('orders'), must_be=">100", name="sufficient_daily_orders")
    daq.check(dqm.mean('response_time'), must_be="<500", name="fast_api_response")
    daq.check(dqm.missing_percentage('email'), must_be="<5", name="email_completeness_95pct")

    # Avoid: Vague names
    daq.check(dqm.count('orders'), must_be=">100", name="check1")
    daq.check(dqm.mean('response_time'), must_be="<500", name="performance")

Appropriate Thresholds
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Consider your specific context
    
    # E-commerce: Different thresholds for different business sizes
    if business_size == "enterprise":
        daq.check(dqm.count('daily_orders'), must_be=">1000", name="enterprise_volume")
    else:
        daq.check(dqm.count('daily_orders'), must_be=">50", name="small_business_volume")
    
    # System monitoring: Different thresholds for different environments
    if environment == "production":
        daq.check(dqm.mean('response_time'), must_be="<200", name="prod_performance")
    else:
        daq.check(dqm.mean('response_time'), must_be="<1000", name="dev_performance")

Tolerance and Flexibility
^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Build in reasonable tolerance
    
    # Too strict: Might cause false alarms
    daq.check(dqm.mean('cpu_usage'), must_be="<50", name="cpu_check")
    
    # Better: Allow for normal variation
    daq.check(dqm.mean('cpu_usage'), must_be="<70", name="cpu_normal")
    daq.check(dqm.max('cpu_usage'), must_be="<90", name="cpu_peak_limit")
    
    # Even better: Use ranges for stability
    daq.check(dqm.mean('cpu_usage'), must_be="[30, 70]", name="cpu_stable_range")

Common Assessment Patterns
--------------------------

System Health Monitoring
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # CPU monitoring
    daq.check(dqm.mean('cpu_percent'), must_be="<80", name="avg_cpu_normal")
    daq.check(dqm.max('cpu_percent'), must_be="<95", name="peak_cpu_limit")
    daq.check(dqm.std('cpu_percent'), must_be="<20", name="cpu_stable")

    # Memory monitoring
    daq.check(dqm.mean('memory_percent'), must_be="<85", name="avg_memory_ok")
    daq.check(dqm.trend('memory_percent', 'timestamp'), must_be="<0.1", name="memory_not_growing")

    # Disk monitoring
    daq.check(dqm.min('disk_free_gb'), must_be=">10", name="sufficient_disk")

Application Performance
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Response time monitoring
    daq.check(dqm.mean('response_ms'), must_be="<500", name="avg_response_fast")
    daq.check(dqm.percentile('response_ms', 95), must_be="<2000", name="p95_response_ok")
    daq.check(dqm.max('response_ms'), must_be="<10000", name="no_timeouts")

    # Error rate monitoring
    error_rate = (dqm.count('errors') / dqm.count('requests')) * 100
    daq.check(error_rate, must_be="<5", name="low_error_rate")

    # Throughput monitoring
    daq.check(dqm.count('requests'), must_be=">1000", name="sufficient_traffic")

Business Metrics
^^^^^^^^^^^^^^^^

.. code-block:: python

    # Revenue monitoring
    daq.check(dqm.sum('revenue'), must_be=">10000", name="daily_revenue_target")
    daq.check(dqm.mean('order_value'), must_be="(20, 500)", name="reasonable_order_size")
    daq.check(dqm.count('transactions'), must_be=">100", name="transaction_volume")

    # User engagement
    daq.check(dqm.distinct_count('user_id'), must_be=">50", name="active_users")
    daq.check(dqm.mean('session_duration'), must_be="(60, 3600)", name="engaged_sessions")

Data Quality Validation
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Completeness checks
    daq.check(dqm.missing_count('user_id'), must_be="==0", name="no_missing_users")
    daq.check(dqm.missing_percentage('email'), must_be="<10", name="email_mostly_complete")

    # Uniqueness checks
    daq.check(dqm.duplicate_count('transaction_id'), must_be="==0", name="unique_transactions")

    # Format validation
    email_pattern = r'^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$'
    daq.check(dqm.pattern_conformance_fraction('email', email_pattern), must_be=">0.9", name="valid_email_format")

Troubleshooting Assessments
---------------------------

Common Issues
^^^^^^^^^^^^^

**Issue**: "Assessment always fails/passes"

**Solutions**:
- Check threshold values against actual data
- Verify measure calculations
- Test with known good/bad data

**Issue**: "Complex assessments are slow"

**Solutions**:
- Simplify assessment logic
- Cache intermediate calculations
- Use string assessments when possible

**Issue**: "Assessments are too sensitive"

**Solutions**:
- Add tolerance ranges
- Use percentiles instead of extremes
- Implement adaptive thresholds

Debugging Assessments
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Test assessments with known values
    def test_assessment():
        # Test your assessment function
        assert is_healthy_response_time(100) == True
        assert is_healthy_response_time(600) == False
        print("Assessment function works correctly")

    test_assessment()

    # Use simple measures to debug
    daq.check(dqm.count('records'), name="debug_count")  # No assessment, just observe
    daq.check(dqm.mean('value'), name="debug_mean")      # See actual values

Next Steps
----------

Now that you understand assessment functions, continue with:

- :doc:`output-handling` - Route assessment results to your systems
- :doc:`configuration` - Advanced configuration for complex scenarios
- :doc:`migration-guide` - Multi-source monitoring patterns

**Pro Tip**: Start with simple string assessments and gradually move to custom functions as your quality requirements become more sophisticated.