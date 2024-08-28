import pathway as pw
from datetime import datetime, timedelta

from dateutil import tz

import artificial_stream_generators
import random


def convert_to_datetime(timestamp: float) -> str:
    return str(datetime.fromtimestamp(timestamp, tz=tz.tzlocal()).strftime(TIME_FORMAT))


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WINDOW_DURATION_SEC = 5
WAIT_FOR_DELAYED_SEC = 1


data = (artificial_stream_generators.generate_artificial_random_viewership_data_stream(number_of_rows=1000)
        .with_columns(date_and_time=pw.apply(convert_to_datetime, pw.this.timestamp))
        .with_columns(date_and_time=pw.this.date_and_time.dt.strptime(TIME_FORMAT)))

data = data.windowby(
        data.date_and_time,
        # data.timestamp,
        window=pw.temporal.tumbling(duration=timedelta(seconds=WINDOW_DURATION_SEC)),
        # window=pw.temporal.tumbling(duration=WINDOW_DURATION_SEC),
        instance=data.user_id,
        behavior=pw.temporal.exactly_once_behavior(shift=timedelta(seconds=WAIT_FOR_DELAYED_SEC)),
).reduce(
        window_start = pw.this._pw_window_start,
        window_end = pw.this._pw_window_end,
        mean_interactions = pw.reducers.avg(pw.this.interaction_events)
)
pw.debug.compute_and_print(data)
