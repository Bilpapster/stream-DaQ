📚 Concepts
================

Welcome to the conceptual heart of Stream DaQ! Understanding these core concepts will help you design effective data quality monitoring for any streaming scenario.

.. admonition:: Why concepts matter
   :class: tip

   Stream processing and data quality monitoring have unique challenges that don't exist in batch processing. These concepts will help you think in "streaming mode" and design monitoring that actually works in real-time environments.

.. grid:: 1 1 2 2
    :gutter: 4
    :padding: 2 2 0 0

    .. grid-item-card:: 🎯 Data Quality for Streams
        :link: data-quality
        :link-type: doc
        :class-header: bg-primary text-white

        **Why streaming data quality is different** - Understand the unique challenges and dimensions of quality in unbounded data streams.

    .. grid-item-card:: 🪟 Stream Windows
        :link: stream-windows
        :link-type: doc
        :class-header: bg-success text-white

        **Bounded computations over unbounded streams** - Learn how tumbling, sliding, and session windows make infinite data streams manageable.

    .. grid-item-card:: 📏 Measures & Assessments
        :link: measures-and-assessments
        :link-type: doc
        :class-header: bg-info text-white

        **Building blocks of quality checks** - Discover how measures extract insights and assessments determine pass/fail criteria.

    .. grid-item-card:: ⚡ Real-time Monitoring
        :link: real-time-monitoring
        :link-type: doc
        :class-header: bg-warning text-dark

        **Stream processing principles** - Understand late arrivals, watermarks, and how Stream DaQ handles the complexity of real-time data.

    .. grid-item-card:: 🎯 Multi-Source Tasks
        :link: multi-source-tasks
        :link-type: doc
        :class-header: bg-primary text-white

        **Task-based architecture** - Monitor multiple independent data sources with different configurations from a single StreamDaQ instance.

    .. grid-item-card:: 📊 Compact vs Native Data
        :link: compact-vs-native-data
        :link-type: doc
        :class-header: bg-secondary text-white

        **Data format strategies** - Learn when to use compact vs native formats and how Stream DaQ handles both seamlessly.

The Big Picture
---------------------

Data quality monitoring in streaming environments involves four key concepts working together:

.. figure:: ../../StreamDaQ_animation.svg
   :width: 100%
   :alt: Stream DaQ concepts overview diagram
   :align: center

   *How Stream DaQ concepts work together*


.. raw:: html

    <object data="StreamDaQ_animation.svg" type="image/svg+xml"></object>


1. **Your streaming data** flows continuously into Stream DaQ
2. **Windows** group data into manageable time-bounded chunks
3. **Measures** extract meaningful metrics from each window
4. **Assessments** evaluate whether those metrics meet your quality standards
5. **Real-time results** flow out as a quality monitoring stream

From Batch to Stream Thinking
---------------------------------------

If you're coming from batch data quality monitoring, here are the key mindset shifts:

.. list-table:: **Batch vs Stream Quality Monitoring**
   :header-rows: 1
   :widths: 30 35 35

   * - Aspect
     - Batch Processing
     - Stream Processing
   * - **Data Scope**
     - Complete dataset
     - Continuous windows
   * - **Quality Assessment**
     - After data is complete
     - As data arrives
   * - **Time Handling**
     - Data is "already there"
     - Must handle late/out-of-order data
   * - **Results**
     - Single quality report
     - Continuous quality stream
   * - **Action**
     - Reprocess if needed
     - Alert and adapt in real-time

Learning Path
--------------------

We recommend exploring these concepts in order:

**1. Start with Data Quality** (:doc:`data-quality`)
   Understanding what makes streaming data quality unique sets the foundation for everything else.

**2. Master Windows** (:doc:`stream-windows`)
   Windows are the key abstraction that makes infinite streams manageable. Get this right, and everything else follows.

**3. Build with Measures & Assessments** (:doc:`measures-and-assessments`)
   Learn the building blocks you'll use to construct your quality monitoring.

**4. Deploy with Real-time Principles** (:doc:`real-time-monitoring`)
   Understand the production considerations for reliable streaming quality monitoring.

Real-World Application
-------------------------------

Each concept page includes:

- 🎯 **Core theory** - What you need to understand
- 💡 **Practical examples** - How it applies to real scenarios
- 🔧 **Stream DaQ implementation** - How to use these concepts with our API
- ⚠️ **Common pitfalls** - What to watch out for
- 🔗 **Related examples** - Links to complete use cases

Quality Monitoring Patterns
-------------------------------------

As you explore these concepts, you'll start recognizing common patterns:

.. grid:: 1 1 2 2
    :gutter: 3

    .. grid-item-card:: **Volume Monitoring**
        :class-header: bg-light

        Track data arrival rates, detect drops or spikes in volume

    .. grid-item-card:: **Value Validation**
        :class-header: bg-light

        Ensure data values fall within expected ranges and formats

    .. grid-item-card:: **Freshness Checking**
        :class-header: bg-light

        Monitor data timeliness and detect stale or delayed data

    .. grid-item-card:: **Consistency Verification**
        :class-header: bg-light

        Check relationships between fields and detect anomalies

    .. grid-item-card:: **Completeness Assessment**
        :class-header: bg-light

        Identify missing data, null values, and incomplete records

    .. grid-item-card:: **Distribution Analysis**
        :class-header: bg-light

        Monitor statistical properties and detect data drift

Ready to dive deeper? Start with :doc:`data-quality` to understand why streaming data quality monitoring is a unique challenge that requires specialized approaches.

.. toctree::
   :hidden:
   :maxdepth: 1

   data-quality
   stream-windows
   measures-and-assessments
   real-time-monitoring
   multi-source-tasks
   compact-vs-native-data
   logging

|made_with_love|