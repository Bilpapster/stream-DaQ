🌊 Stream-first Data Quality
====================================

Data quality has been a cornerstone of data management for decades, but streaming data introduces unique challenges that
traditional quality frameworks struggle to address. Let's explore how data quality concepts evolve when data never
stops flowing.

Traditional Data Quality Dimensions
-----------------------------------

The data quality literature has established several fundamental dimensions that define what makes data "good":

.. grid:: 1 1 2 3
    :gutter: 3

    .. grid-item-card:: **Accuracy**
        :class-header: bg-primary text-white

        Data correctly represents real-world entities and relationships

    .. grid-item-card:: **Completeness**
        :class-header: bg-success text-white

        All required data is present without missing values

    .. grid-item-card:: **Consistency**
        :class-header: bg-info text-white

        Data follows defined rules and constraints across the dataset

    .. grid-item-card:: **Validity**
        :class-header: bg-warning text-dark

        Data conforms to defined formats, types, and value ranges

    .. grid-item-card:: **Uniqueness**
        :class-header: bg-secondary text-white

        No unwanted duplicates exist in the dataset

    .. grid-item-card:: **Timeliness**
        :class-header: bg-danger text-white

        Data is available when needed and reflects current state

These dimensions provide a solid conceptual framework, but implementing them in practice reveals significant challenges.

The Implementation Gap
-----------------------

Our research into seven widely-used open-source data quality tools
`revealed a fascinating problem <https://arxiv.org/abs/2507.17507>`_:
**there's no standard mapping between conceptual quality
dimensions and actual code implementations**.

.. admonition:: Research Insight
   :class: note

   We analyzed the source code of Deequ, Great Expectations, and five other popular tools.
   What we found was surprising: each tool implements quality checks differently and often uses different terminology
   for the same concepts. This makes the landscape fragmented and the mapping between DQ dimensions
   and tool functionality a many-to-many relationship. If you want to learn more, check out our
   `research paper <https://arxiv.org/abs/2507.17507>`_.

**The Many-to-Many Problem**

Consider how different tools handle "completeness":

- **Tool A** checks the number NULL values in a column (completeness as absence of data);
- **Tool B** chekcs the fraction of NULL values in a column;
- **Tool C** measures record counts (completeness as volume);
- **Tool D** combines all three approaches, using different names and variations.

Meanwhile, a single function might address multiple dimensions:

.. todo find a better example here

- A "range check" validates **validity** (correct format) and **consistency** (business rules)
- A "duplicate detection" ensures **uniqueness** and might improve **accuracy**

This fragmentation makes it difficult to:

- Compare quality across tools
- Build comprehensive quality monitoring
- Understand what's actually being measured

Streaming Data Quality: New Challenges
--------------------------------------

When data becomes a continuous stream, traditional quality dimensions take on new meanings and face new limitations:

Completeness Gets Complex
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Traditional completeness** focuses on missing values within a dataset:

.. code-block:: python

    # Static data completeness
    missing_values = df.isnull().sum()
    completeness_rate = 1 - (missing_values / total_records)

**Streaming completeness** must consider temporal expectations:

.. code-block:: python

    # Stream DaQ completeness considers frequency
    daq.add(dqm.count('sensor_readings'),
            assess=">50",  # Expect >50 readings per minute
            name="temporal_completeness")

In streams, completeness means:
- ✅ **Value completeness**: No NULL/missing fields (traditional)
- ✅ **Temporal completeness**: Expected data arrival frequency
- ✅ **Sequence completeness**: No gaps in expected record sequences

Accuracy Without Gold Standards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Traditional accuracy** compares data against a known "gold standard":

.. code-block:: python

    # Batch accuracy: compare against reference dataset
    accuracy = (df == gold_standard).mean()

**Streaming accuracy** faces a fundamental problem: **you can't maintain a complete gold standard for unbounded data streams**. Instead, we rely on:

- **Statistical consistency**: Values follow expected distributions
- **Cross-validation**: Multiple sources confirm the same measurements
- **Business rule compliance**: Data satisfies known constraints
- **Temporal consistency**: Values change in predictable ways

Real-Time Timeliness
~~~~~~~~~~~~~~~~~~~~~~

**Traditional timeliness** measures data freshness at query time:

.. code-block:: python

    # How old is this data?
    data_age = current_time - data.last_updated

**Streaming timeliness** requires continuous monitoring:

.. code-block:: python

    # Stream DaQ monitors arrival delays in real-time
    daq.add(dqm.max_delay('timestamp'),
            assess="<30",  # Max 30 seconds delay allowed
            name="arrival_timeliness")

Stream-Native Quality Dimensions
--------------------------------

Based on our analysis of streaming challenges, we've identified additional quality dimensions that are unique to continuous data:

.. list-table:: **Extended Quality Dimensions for Streams**
   :header-rows: 1
   :widths: 20 40 40

   * - Dimension
     - Definition
     - Stream DaQ Example
   * - **Velocity**
     - Data arrives at expected rates
     - ``dqm.count() assess=">100"``
   * - **Ordering**
     - Events arrive in expected sequence
     - ``dqm.is_ordered('timestamp')``
   * - **Latency**
     - Processing delay stays within bounds
     - ``dqm.processing_delay() assess="<5.0"``
   * - **Continuity**
     - No unexpected gaps in the stream
     - ``dqm.time_gap() assess="<60"``

Stream DaQ's Comprehensive Approach
-----------------------------------

Rather than forcing you to navigate the fragmented landscape of quality tools, Stream DaQ provides a **unified, stream-native quality suite** built from the ground up for continuous data.

Unified Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~

We've analyzed quality measures from seven major tools and **consolidated them into a single, coherent framework**:

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **30+ Built-in Measures**
        :class-header: bg-primary text-white

        All the functionality from major tools, unified under consistent API

    .. grid-item-card:: **Stream-Adapted Algorithms**
        :class-header: bg-success text-white

        Every measure thoughtfully adapted from batch to streaming contexts

    .. grid-item-card:: **Standardized Terminology**
        :class-header: bg-info text-white

        Clear, consistent naming that maps to quality dimensions

    .. grid-item-card: **Extensible Framework**
        :class-header: bg-warning text-dark

        Easy to add custom measures for domain-specific quality needs

Dimensional Coverage
~~~~~~~~~~~~~~~~~~~~~~~~

Stream DaQ's measures comprehensively cover all traditional and streaming-specific quality dimensions:

.. code-block:: python

    from streamdaq import StreamDaQ, DaQMeasures as dqm

    # Traditional dimensions adapted for streams
    daq.add(dqm.null_count('field'), assess="==0", name="completeness_nulls") \
       .add(dqm.count('records'), assess=">100", name="completeness_volume") \
       .add(dqm.range_compliance('value', 0, 100), assess=">=0.95", name="validity_range") \
       .add(dqm.unique_count('id'), assess=lambda x: x == dqm.count('id'), name="uniqueness") \
       .add(dqm.correlation('field1', 'field2'), assess="(0.7, 1.0)", name="consistency")

    # Stream-specific dimensions
       .add(dqm.arrival_rate('timestamp'), assess="(50, 200)", name="velocity") \
       .add(dqm.is_ordered('timestamp'), assess=True, name="ordering") \
       .add(dqm.max_delay('timestamp'), assess="<30", name="timeliness")

Research-Backed Design
~~~~~~~~~~~~~~~~~~~~~~~~

Our approach is grounded in systematic analysis of existing tools and streaming data challenges:

.. admonition:: Academic Foundation
   :class: tip

   Stream DaQ's design is informed by peer-reviewed research analyzing the gap between data quality theory and practice. Every measure in our suite has been carefully considered for its theoretical foundation and practical utility in streaming contexts.

**Key Design Principles:**

1. **Dimension-Complete**: Cover all established and emerging quality dimensions
2. **Stream-Native**: Built for continuous data from the ground up
3. **Practically Focused**: Bridge the theory-implementation gap
4. **Terminology-Consistent**: Use clear, standardized naming
5. **Performance-Optimized**: Efficient for high-volume streams

Common Streaming Quality Patterns
---------------------------------

Understanding how traditional dimensions manifest in streaming contexts helps you design better quality monitoring:

**Volume-Based Completeness**

.. code-block:: python

    # Monitor expected data arrival rates
    daq.add(dqm.count('events'), assess="(100, 1000)", name="volume_completeness")

**Statistical Accuracy**

.. code-block:: python

    # Ensure values follow expected distributions
    daq.add(dqm.mean('temperature'), assess="(18, 25)", name="statistical_accuracy") \
       .add(dqm.std('temperature'), assess="<3.0", name="variance_consistency")

**Temporal Validity**

.. code-block:: python

    # Validate timestamps and detect time-based anomalies
    daq.add(dqm.is_ordered('timestamp'), assess=True, name="temporal_validity") \
       .add(dqm.max_time_gap('timestamp'), assess="<300", name="continuity")

**Cross-Stream Consistency**

.. code-block:: python

    # Check relationships between different data streams
    daq.add(dqm.correlation('stream_a_value', 'stream_b_value'),
            assess="(0.8, 1.0)",
            name="cross_stream_consistency")

What's Next?
------------

Now that you understand how data quality concepts evolve for streaming data:

- 📊 **Understand data formats**: :doc:`compact-vs-native-data` - How Stream DaQ handles different data representations seamlessly
- 🪟 **Learn about windowing**: :doc:`stream-windows` - How to make infinite streams manageable
- 📏 **Explore measures**: :doc:`measures-and-assessments` - The building blocks of Stream DaQ quality checks
- 💡 **See it in action**: :doc:`../examples/index` - Real-world quality monitoring examples