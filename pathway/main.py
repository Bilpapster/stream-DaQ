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

daq.add(dqm.count('items'), "count") \
    .add(dqm.min('items'), "min") \
    .add(dqm.max('items'), "max") \
    .add(dqm.median('items'), "std") \
    .add(dqm.most_frequent('items'), "max") \
    .add(dqm.number_of_distinct('items'), "distinct")

daq.watch_out()
