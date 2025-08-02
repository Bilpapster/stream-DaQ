TL; DR
------

Plug-and-play quality monitoring for high-volume and velocity data
streams! In Python, of course!

.. code:: python

   # pip install streamdaq

   from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
   from some_existing_workflow import check_most_frequent_items

   daq = StreamDaQ().configure(
       window=Windows.tumbling(3),
       instance="user_id",
       time_column="timestamp",
       wait_for_late=1,
       time_format='%Y-%m-%d %H:%M:%S'
   )

   # Step 2: Define what Data Quality means for you
   daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count")
       .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact")
       .add(dqm.most_frequent('interaction_events'), assess=check_most_frequent_items, name="freq_interact")
       .add(dqm.distinct_count_approx('interaction_events'), assess="==9", name="approx_dist_interact")

   # Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py

   # Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
   daq.watch_out()

More examples can be found in the `examples
directory <https://github.com/Bilpapster/stream-DaQ/tree/main/examples>`__
of the project. Even more examples are on their way to be integrated
shortly! Thank you for your patience!


