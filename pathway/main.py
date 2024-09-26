from StreamDaQ import StreamDaQ
from DaQMeasures import DaQMeasures as dqm
from Windows import tumbling
from datetime import timedelta

daq = StreamDaQ().configure(
    window=tumbling(timedelta(seconds=20)),
    instance="user_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

daq.add(dqm.count('interaction_events'), "count") \
    .add(dqm.min('interaction_events'), "min") \
    .add(dqm.max('interaction_events'), "max") \
    .add(dqm.median('interaction_events'), "median") \
    .add(dqm.most_frequent('interaction_events'), "frequent") \
    .add(dqm.number_of_distinct_approx('interaction_events'), "distinct_approx") \
    .add(dqm.number_of_distinct('interaction_events'), "distinct")

daq.watch_out()
