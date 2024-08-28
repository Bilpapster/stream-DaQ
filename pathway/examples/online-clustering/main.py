import pathway as pw
from datetime import datetime, timedelta
from typing import List, Tuple

from dateutil.tz import tz

# DATA_FILE = 'm2_episode_3_log_debug.csv'
DATA_FILE = 'm2_episode_3_log_actual.csv'
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
SINK_FILE_NAME = "online_clustering_sink.csv"
WINDOW_DURATION_SEC = 1000


def feed_sand(items: list):
    print(f"Apostolos do your magic - feed sand with {items}")
    print(f"Count: {len(items)}")
    print()
    return True


def convert_to_datetime(timestamp: float) -> str:
    return str(datetime.fromtimestamp(timestamp, tz=tz.tzlocal()).strftime(TIME_FORMAT))



def sort_in_parallel(timestamps: List[float], items: List[float]) -> List[float]:
    """
    Sorts timestamps and items in parallel, based on timestamps.
    :param timestamps: a list of timestamps as floats
    :param items: a list of items as floats, corresponding to the timestamps
    :return: the list of items sorted, based on their timestamps
    """
    import numpy as np

    sorted_indices = np.argsort(timestamps)
    timestamps_sorted = timestamps[sorted_indices]
    items_sorted = items[sorted_indices]
    return items_sorted # we care only about items for now, but we can also use timestamps_sorted if needed


class ResultSchema(pw.Schema):
    result: bool


class TimeSeriesEventSchema(pw.Schema):
    press_id: str
    timestamp: float
    value: float


data = (pw.demo.replay_csv_with_time(DATA_FILE, schema=TimeSeriesEventSchema, time_column='timestamp')
        .with_columns(date_and_time=pw.apply(convert_to_datetime, pw.this.timestamp))
        .with_columns(date_and_time=pw.this.date_and_time.dt.strptime(TIME_FORMAT))
        )

# data = pw.debug.table_from_markdown(
#     """
#       | press_id  | timestamp           | value
#    0  | m2        | 2024-07-25T18:11:58 | 1
#    1  | m2        | 2024-07-25T18:12:00 | 2
#    2  | m2        | 2024-07-25T18:12:02 | 3
#    3  | m2        | 2024-07-25T18:12:04 | 4
#    4  | m2        | 2024-07-25T18:12:06 | 5
#    5  | m2        | 2024-07-25T18:12:08 | 6
#    6  | m2        | 2024-07-25T18:12:10 | 7
#    7  | m2        | 2024-07-25T18:12:12 | 8
#    8  | m2        | 2024-07-25T18:12:14 | 9
#    9  | m2        | 2024-07-25T18:12:16 | 10
#    10 | m2        | 2024-07-25T18:12:18 | 11
#    11 | m2        | 2024-07-25T18:12:20 | 12
#    12 | m2        | 2024-07-25T18:12:22 | 13
#    13 | m2        | 2024-07-25T18:12:24 | 14
#     """
# ).with_columns(timestamp=pw.this.timestamp.dt.strptime(TIME_FORMAT))

result = data.windowby(
    data.date_and_time,
    window=pw.temporal.tumbling(duration=timedelta(seconds=WINDOW_DURATION_SEC)),
    instance=data.press_id,
    behavior=pw.temporal.exactly_once_behavior(shift=timedelta(seconds=3)),
).reduce(
    # pw.this._pw_window,
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    min_t=pw.reducers.min(pw.this.timestamp),
    max_t=pw.reducers.max(pw.this.timestamp),
    count=pw.reducers.count(pw.this.timestamp),
    max_value=pw.reducers.max(pw.this.value),
    min_value=pw.reducers.min(pw.this.value),
    items=pw.reducers.ndarray(pw.this.value),
    timestamps=pw.reducers.ndarray(pw.this.timestamp),
    dates_and_time=pw.reducers.ndarray(pw.this.date_and_time).to_string(),
    # status=pw.apply(feed_sand, pw.reducers.ndarray(pw.this.value))
).with_columns(
    sorted_items_by_timestamp=pw.apply(sort_in_parallel, pw.this.timestamps, pw.this.items).to_string()
)

pw.io.csv.write(result, SINK_FILE_NAME)
# pw.debug.compute_and_print(result, include_id=False)
pw.run()
