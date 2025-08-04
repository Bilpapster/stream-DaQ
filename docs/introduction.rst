The Stream DaQ Manifesto
========================

.. epigraph::

   *"Data changes, all the time."*

   -- Divesh Srivastava, *Head of Database Research at AT&T*

In fact, not only data, but also **its quality changes**, all the time. What used to be a reliable data source an hour ago may be sending you garbage at this very moment — and vice versa. Not the same, predictable garbage, however, since **data errors also change**, all the time. Error types (missing or incorrect values), durations (short- or long-lasting), and patterns (one-off, periodic, or just reoccurring) constantly shapeshift in ways that can catch even the most prepared teams off guard. This is what we call the *streaming data quality challenge*.

The streaming data quality challenge
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Picture this: A frozen sensor keeps sending the same temperature reading over and over again, silently waiting for a reboot or replacement. If you are lucky enough, this frozen reading lies entirely within the accepted range of temperatures. Your super-intelligent machine leaning model relies on this data to predict equipment failures, but everything seems fine, isn't it? *After all, the temperature is stable, right?*

Then, a network hiccup causes a drop in data volume. Now your super-intelligent model is deprived of the data it needs to do what it knows best: highly accurate predictions. Luckily, the network recovers, enabling old cached values, which lost their way during the outage, to suddenly flood back in, mixing fresh data with stale, potentially duplicate records. *Not exactly what you promised your super-intelligent model, is it?*

Such issues, and many more, happen in production data pipelines, all the time. They occur in ways that are:

- **Hard to predict**: Quality issues emerge from the complex interaction of systems, networks, data sources, and programming bugs;
- **Hard to detect**: Traditional batch-oriented quality checks can miss or misdetect real-time quality issues in streams, primarily because they are designed to solve a different problem;
- **Even harder to fix**: Without immediate visibility, issues can propagate through the pipeline, causing downstream failures that take valuable time and resources to root out and resolve.

Real-time Impact, Real-time Solution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In streaming environments, data quality issues don't wait for your next batch job to run. **This is exactly why we built Stream DaQ**: to give you awareness of a broad spectrum of data quality issues in your streams *the moment they happen*. Not minutes later. Not in the next batch run. *Right now*, so that you can focus on what matters most: making informed decisions based on reliable data. **All the time.**

With Stream DaQ, you can:

- **Define quality on your terms**: Defining what *good data* means varies by use case, industry, and business context. That's why Stream DaQ comes with **30+ built-in quality measures** to cherry pick from. If you need something more specific, just write your own logic **in Python**, and Stream DaQ will handle the rest!
- **Monitor continuously**: Stream DaQ vigilantly performs continuous data quality assessment in real-time as data flows through your pipeline.
- **React immediately**: Stream DaQ generates **configurable alerts**, so that you can take informed action to prevent downstream impact.
- **Focus on what matters**: Spend time on insights and decisions, not on manual data validation. Stream DaQ will let you know when quality degrades, **the moment it happens**.

.. admonition:: The Stream DaQ Philosophy
   :class: note

   Quality monitoring shouldn't be an afterthought or a separate system. It should be as natural and integrated as any other part of your data pipeline — simple to set up, reliable to run, and powerful enough to catch the issues that matter most to your business.

Whether you're a data engineer building robust pipelines, a data scientist ensuring model reliability, or a business analyst maintaining dashboard accuracy, Stream DaQ helps you sleep better knowing your data streams are under vigilant, intelligent watch.

**All the time.**