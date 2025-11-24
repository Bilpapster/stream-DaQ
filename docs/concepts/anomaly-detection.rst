🚨 Anomaly Detection
====================

Stream DaQ's anomaly detection capabilities provide automated identification of unusual patterns and outliers in streaming data without requiring manual threshold configuration. This statistical approach complements traditional rule-based monitoring by detecting subtle deviations that static thresholds might miss.

Core Principles
---------------

**Statistical Baseline Learning**

Anomaly detection works by establishing a statistical baseline from historical data windows. The system continuously learns the normal operating characteristics of your data streams, including:

- **Central Tendencies**: Mean, median values for each monitored measure
- **Variability Patterns**: Standard deviation, variance, and distribution shape

**Z-Score Based Detection**

The system uses Z-score analysis to identify anomalies:

.. math::

   Z = \frac{x - \mu}{\sigma}

Where:
- ``x`` = Current measure value
- ``μ`` = Historical mean of the measure
- ``σ`` = Historical standard deviation

Values with ``|Z| > threshold`` are flagged as anomalies.

**Adaptive Thresholding**

Unlike static thresholds that require manual tuning, statistical detection adapts to:

- **Seasonal Patterns**: Automatically adjusts baselines for cyclic data
- **Trend Changes**: Adapts to gradual shifts in operational parameters
- **Data Volume Variations**: Handles varying data densities across time periods
- **Multi-Modal Distributions**: Works with complex data distributions

Detection Architecture
----------------------

**Buffer-Based Learning**

.. code-block:: text

    Window 1  Window 2  Window 3  ...  Window N    Current Window
    [baseline computation buffer]           [anomaly detection]

- **Buffer Size**: Number of historical windows used for baseline statistics
- **Warmup Period**: Initial windows processed before detection begins
- **Rolling Updates**: Baseline continuously updated with new data

**Multi-Measure Monitoring**

Stream DaQ can monitor multiple statistical measures simultaneously:

- **Simple Measures**: ``min``, ``max``, ``mean``, ``std``, ``count``
- **Complex Measures**: Custom combinations of basic statistics
- **Cross-Column Analysis**: Measures applied across multiple data columns
- **Temporal Measures**: Time-based patterns and trends

**Top-K Prioritization**

When multiple anomalies occur simultaneously, the top-K mechanism prioritizes the most significant deviations:

- **Anomaly Scoring**: Ranks detected anomalies by Z-score magnitude
- **Focus Management**: Reports only the most critical issues per window
- **Noise Reduction**: Filters out minor statistical variations

Configuration Strategies
------------------------

**Threshold Selection**

.. list-table::
   :widths: 15 25 60
   :header-rows: 1

   * - Threshold
     - Sensitivity
     - Use Case
   * - 1.5
     - High (sensitive)
     - Early warning
   * - 2.0
     - Moderate
     - Balanced detection for general monitoring
   * - 2.5
     - Low (conservative)
     - Critical systems where false positives are costly
   * - 3.0
     - Very Low
     - Regulatory compliance, safety-critical applications

**Buffer Size Guidelines**

- **Small Buffers (5-10 windows)**: Fast adaptation, suitable for rapidly changing environments
- **Medium Buffers (10-20 windows)**: Balanced stability and responsiveness
- **Large Buffers (20+ windows)**: Stable baselines, suitable for slowly changing systems

**Warmup Time Considerations**

- **Short Warmup (1-2 windows)**: Quick detection start, may have initial false positives
- **Medium Warmup (3-5 windows)**: Balanced accuracy and response time
- **Long Warmup (5+ windows)**: High accuracy, delayed detection start

Use Cases and Applications
--------------------------

**Industrial IoT and Manufacturing**

- **Equipment Health**: Detect bearing wear, motor degradation, temperature anomalies
- **Process Optimization**: Identify deviations from optimal operating conditions

**Financial Services**

- **Fraud Detection**: Identify unusual transaction patterns and behaviors
- **Market Monitoring**: Detect abnormal trading volumes or price movements
- **Risk Management**: Monitor portfolio risk metrics for unusual patterns

**Smart Cities and Infrastructure**

- **Traffic Monitoring**: Detect unusual congestion patterns or accidents
- **Environmental Monitoring**: Identify air quality anomalies or sensor malfunctions
- **Energy Management**: Detect unusual consumption patterns or grid instabilities

**Healthcare and Life Sciences**

- **Patient Monitoring**: Detect abnormal vital sign patterns
- **Medical Device QC**: Monitor device performance and calibration drift
- **Clinical Trial Data**: Identify data quality issues or protocol deviations

Combining with Rule-Based Monitoring
------------------------------------

Statistical anomaly detection works best when combined with traditional rule-based checks:

**Complementary Approaches**

.. code-block:: python

    # Rule-based: Known failure conditions
    task.check(dqm.min('temperature'), must_be=">0", name="temp_above_zero")
    task.check(dqm.max('pressure'), must_be="<1000", name="pressure_limit")

    # Statistical: Unknown patterns and drift
    detector = StatisticalDetector(
        measures=[("mean", "temperature"), ("std", "pressure")],
        threshold=2.0
    )
**When to Use Each Approach**

- **Rule-Based**: Known business rules, regulatory compliance, safety thresholds
- **Statistical**: Unknown failure patterns, gradual drift, complex interactions
- **Combined**: Comprehensive monitoring with both known and unknown failure detection

Performance and Scalability
----------------------------

**Memory Usage**

- Buffer size directly impacts memory consumption
- Each measure-column combination maintains separate statistics
- Consider data retention policies for long-running systems

**Real-Time Performance**

- Low latency anomaly detection suitable for real-time systems
- Configurable warmup periods balance accuracy and response time
- Top-K reporting reduces alert volume and processing overhead

For practical implementation examples, see :doc:`../examples/advanced-examples` and the ``examples/anomaly_detection.py`` file.