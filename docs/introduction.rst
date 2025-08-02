Introduction
====================================

.. epigraph::

   *"Data changes, all the time.''*

   -- Divesh Srivastava, *Head of Database Research at AT&T*


In fact, not only data, but also **its quality** changes, all the time. What used to be a good data source yesterday may send you garbage today and vice versa. To get things worse, **data errors** also change, all the time. At first, a stuck sensor sends the same value over and over again, silently shouting for a reboot or replacement. Then, a network issue causes a drop in the data volume, depriving your super-intelligent machine learning model of the data it needs to make accurate predictions. When the network is back, the old, outdated values, which had lost their way, assume that it's their time to shine. So your super-intelligent model now sees the fresh values, mixed up with old, stale, and duplicate one. Not really what it was expecting for, is it?

These, and many more issues, happen in production data pipelines, all the time. And they happen in a way that is hard to predict, hard to detect, and even harder to fix without a vigilant quality monitoring layer in place. We have designed Stream DaQ for this exact purpose: to keep you aware of the data quality changes in your data streams *the moment they happen*. So that you can focus on what matters the most: making decisions based on accurate and reliable data. **All the time**.



.. admonition:: Stream DaQ at a glance

   Stream DaQ allows you to monitor the quality your data streams in just a few lines of Python code. **For free and with** `open-sourced <https://github.com/bilpapster/stream-daq>`_ **code**. If you want to learn more, you are in the right place! Use the navigation bar on the left to explore the documentation, or jump straight to the `TL; DR <#tl-dr>`__ section for a quick start guide.
