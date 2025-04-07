# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows

daq = StreamDaQ().configure(
    window=Windows.tumbling(20),
    # instance="user_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

# Step 2: Define what Data Quality means for you
daq.add(dqm.count('interaction_events'), "count") \
    .add(dqm.min('interaction_events'), "min") \
    .add(dqm.max('interaction_events'), "max") \
    .add(dqm.median('interaction_events'), "median") \
    .add(dqm.most_frequent('interaction_events'), "frequent") \
    .add(dqm.number_of_distinct_approx('interaction_events'), "distinct_approx") \
    .add(dqm.number_of_distinct('interaction_events'), "distinct")

# Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
