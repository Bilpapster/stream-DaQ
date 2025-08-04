================================
Stream DaQ Documentation
================================

**Stream DaQ is a free and open-source Python library** that makes data quality monitoring for streaming data as simple as a few lines of code. Monitor your data streams in real time, get instant alerts when quality issues arise, and keep your data pipelines running smoothly.

.. image:: https://github.com/user-attachments/assets/ebe3a950-5fbb-49d8-b6b1-f232ca7dc362
   :alt: Stream DaQ Logo
   :align: center
   :width: 300px

   Quacklity, the Stream DaQ mascot.

.. grid:: 1 2 2 2
    :gutter: 4
    :padding: 2 2 0 0
    :class-container: sd-text-center

    .. grid-item-card:: 👋 Our Manifesto
        :link: introduction
        :link-type: doc
        :class-header: bg-light

        Understand what Stream DaQ is all about

    .. grid-item-card:: 🚀 Quick Start
        :link: getting-started/quickstart
        :link-type: doc
        :class-header: bg-light

        Get up and running in less than 5 minutes

    .. grid-item-card:: 💡 Examples
        :link: examples/index
        :link-type: doc
        :class-header: bg-light

        Explore real-world examples and use cases

    .. grid-item-card:: 📚 Concepts
        :link: concepts/index
        :link-type: doc
        :class-header: bg-light

        Learn how data quality works for streaming data

    .. grid-item-card:: 📖 API Reference
        :link: api-reference
        :link-type: doc
        :class-header: bg-light

        Complete API documentation

    .. grid-item-card:: 💌 Contributing
        :link: contributing
        :link-type: doc
        :class-header: bg-light

        Help make Stream DaQ a better tool



Installation
------------

.. code-block:: bash

    pip install streamdaq

**Requirements**: Python >= 3.11


TL;DR
-----

.. code-block:: python

    # pip install streamdaq

    from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

    # Step 1: Configure your monitoring setup
    daq = StreamDaQ().configure(
        window=Windows.tumbling(3),
        instance="user_id",
        time_column="timestamp",
        wait_for_late=1,
        time_format='%Y-%m-%d %H:%M:%S'
    )

    # Step 2: Define what Data Quality means for you
    daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
       .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
       .add(dqm.most_frequent('interaction_events'), assess=check_most_frequent_items, name="freq_interact")

    # Step 3: Start monitoring and let Stream DaQ do the work
    daq.watch_out()

Key Features
-------------

.. grid:: 1 1 2 3
    :gutter: 4
    :padding: 2 2 0 0

    .. grid-item-card:: ⚡ Real-time Monitoring
        :class-header: bg-primary text-white

        Get instant alerts when your data quality drops below your defined thresholds

    .. grid-item-card:: 🔧 Highly Configurable
        :class-header: bg-primary text-white

        Choose from 30+ built-in quality measures or create your own in plain Python

    .. grid-item-card:: 🪟 Flexible Windows
        :class-header: bg-primary text-white

        Support for tumbling, sliding, and session-based windows to fit your use case

    .. grid-item-card:: 🎯 Stream-Native
        :class-header: bg-primary text-white

        Built specifically to address the challenges of unbounded streams

    .. grid-item-card:: 🐍 Pure Python
        :class-header: bg-primary text-white

        If you can write Python, you can monitor your data streams with Stream DaQ

    .. grid-item-card:: 📊 Rich Output
        :class-header: bg-primary text-white

        Check results flow as a stream themselves, ready for further processing or alerting

.. admonition:: Perfect for
   :class: tip

   - **Data Engineers** building robust, end-to-end streaming pipelines
   - **Data Scientists** ensuring model input quality
   - **MLOps Engineers** monitoring production data flows
   - **Analytics Teams** maintaining dashboard reliability
   - **Data Enthousiasts** exploring the state-of-the-art in data quality

Next Steps
-----------

.. toctree::
   :hidden:
   :maxdepth: 2

   introduction
   why-stream-daq
   concepts/index
   getting-started/index
   examples/index
   user-guide/index
   api-reference
   contributing
   changelog

Ready to dive in? Here are some suggested paths:

**New to Stream DaQ?** → Start with :doc:`introduction`

**Starving for action?** → Jump straight to the :doc:`getting-started/quickstart`

**Eager to deepen understanding?** → Read :doc:`concepts/index`

**Looking for examples?** → Check out :doc:`examples/index`

**Need detailed configuration?** → Browse :doc:`user-guide/index`

Support & Community
-------------------

We are a small, dedicated team committed to making Stream DaQ the best it can be. **Stream DaQ is and will always be free and open-source.** We really appreciate your support in making this project better. Here are some ways you can help:

- 🐛 **Report bugs**: `GitHub Issues <https://github.com/bilpapster/stream-DaQ/issues>`_
- 💬 **Ask questions**: `GitHub Discussions <https://github.com/bilpapster/stream-DaQ/discussions>`_
- ⭐ **Star the project**: `GitHub Repository <https://github.com/bilpapster/stream-DaQ>`_
- 📧 **Contact the team**:
    - papster at csd.auth.gr - Vassilis, primary maintainer 👷‍♂️
    - gounaria at the same domain - Anastasios, project supervisor 🦸

Acknowledgments
---------------

Stream DaQ is developed by the Data Engineering (DELAB) Team of `Datalab AUTh <https://datalab.csd.auth.gr/>`_,
under the supervision of `Prof. Anastasios Gounaris <https://datalab-old.csd.auth.gr/~gounaris/>`_. Special thanks to `Maria Kavouridou <https://www.linkedin.com/in/maria-kavouridou/>`_ for giving birth to Quacklity, the Stream DaQ mascot!

