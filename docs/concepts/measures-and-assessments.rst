📏 Measures and Assessments
==============================

Measures and assessments are the building blocks of Stream DaQ quality monitoring. Understanding how they work together helps you design effective quality checks for any streaming scenario.

The Two-Step Process
--------------------

Every quality check in Stream DaQ follows a simple pattern:

1. **Measure**: Extract a numeric value from your data window
2. **Assess**: Determine if that value meets your quality criteria

.. code-block:: python

    # Step 1: Measure (what to calculate)
    measure = dqm.count('events')
    
    # Step 2: Assess (what makes it "good")
    assessment = ">100"
    
    # Combined: Complete quality check
    daq.check(measure, must_be=assessment, name="sufficient_volume")

What Are Measures?
--------------------

**Measures** are functions that extract meaningful numeric insights from data windows. They answer questions like:

- How many records are in this window?
- What's the average response time?
- Are there any missing values?
- Do values fall within expected ranges?

**Common Measure Categories:**

.. code-block:: python

    # Volume measures
    dqm.count('records')           # Total record count
    dqm.distinct_count('user_id')  # Unique users
    
    # Statistical measures  
    dqm.mean('response_time')      # Average response time
    dqm.percentile('latency', 95)  # 95th percentile latency
    
    # Quality-specific measures
    dqm.missing_count('email')     # Count of null values
    dqm.duplicate_count('id')      # Count of duplicates

What Are Assessments?
------------------------

**Assessments** define your quality criteria. They take measure results and return pass/fail decisions.

**Simple String Assessments:**

.. code-block:: python

    # Comparison operators
    daq.check(dqm.count('events'), must_be=">100", name="volume")
    daq.check(dqm.mean('temp'), must_be="<30", name="temperature")
    
    # Range checks
    daq.check(dqm.std('latency'), must_be="(0, 50)", name="stability")
    daq.check(dqm.error_rate(), must_be="[0, 5]", name="errors")

**Custom Assessment Functions:**

.. code-block:: python

    def is_healthy_system(cpu_avg, memory_max, error_count):
        return cpu_avg < 80 and memory_max < 90 and error_count == 0
    
    # Apply custom logic
    system_health = pw.apply_with_type(
        is_healthy_system, bool,
        dqm.mean('cpu'), dqm.max('memory'), dqm.count('errors')
    )
    daq.check(system_health, must_be="==True", name="system_health")

Combining Measures
--------------------

You can combine measures using arithmetic operations:

.. code-block:: python

    # Calculate error rate
    error_rate = dqm.count('errors') / dqm.count('requests') * 100
    daq.check(error_rate, must_be="<5", name="error_rate_percent")
    
    # Data completeness score
    completeness = (dqm.count('valid') / dqm.count('total')) * 100
    daq.check(completeness, must_be=">95", name="completeness_score")

Choosing the Right Measures
---------------------------

**For Volume Monitoring:**
- ``count()`` - Total records
- ``distinct_count()`` - Unique entities

**For Value Validation:**
- ``range_conformance_fraction()`` - Values within bounds
- ``pattern_conformance_fraction()`` - Format validation

**For Statistical Quality:**
- ``mean()``, ``std()`` - Central tendency and spread
- ``percentile()`` - Outlier detection

**For Data Integrity:**
- ``missing_count()`` - Completeness
- ``duplicate_count()`` - Uniqueness

Assessment Patterns
---------------------

**Threshold-Based:**
.. code-block:: python

    daq.check(dqm.count('orders'), must_be=">50", name="min_orders")

**Range-Based:**
.. code-block:: python

    daq.check(dqm.mean('price'), must_be="(10, 1000)", name="price_range")

**Business Logic:**
.. code-block:: python

    def revenue_check(daily_revenue, order_count):
        return daily_revenue > 1000 and order_count > 10
    
    daq.check(revenue_logic, must_be="==True", name="business_health")

Next Steps
----------

- :doc:`../user-guide/measures` - Complete guide to all available measures
- :doc:`../user-guide/assessment-functions` - Detailed assessment patterns
- :doc:`../examples/index` - See measures and assessments in real scenarios