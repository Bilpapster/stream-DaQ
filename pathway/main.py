from StreamDaQ import StreamDaQ
from DaQMeasures import DaQMeasures as dqm
from Windows import tumbling
from datetime import timedelta

daq = StreamDaQ().configure(
    window=tumbling(duration=timedelta(hours=1)),
    instance="user_id",
    wait_for_late=timedelta(seconds=10),
    time_format="%H:%M:%S"
)

daq.add(dqm.count('interaction_events'), "count") \
    .add(dqm.min('interaction_events'), "min") \
    .add(dqm.max('interaction_events'), "max") \
    .add(dqm.median('interaction_events'), "std") \
    .add(dqm.most_frequent('interaction_events'), "max") \
    .add(dqm.number_of_distinct('interaction_events'), "distinct")

daq.watch_out()
