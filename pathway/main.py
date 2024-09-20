from StreamDaQ import StreamDaQ
from DaQMeasures import DaQMeasures as measures

daq = StreamDaQ()
daq.add(measures.count('items'), "count") \
    .add(measures.min('items'), "min") \
    .add(measures.max('items'), "max") \
    .add(measures.median('items'), "std") \
    .add(measures.most_frequent('items'), "max") \
    .add(measures.number_of_distinct('items'), "distinct") \
    .watch_out()
