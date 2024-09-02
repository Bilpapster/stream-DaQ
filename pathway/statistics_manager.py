import pathway as pw
from datetime import datetime, timedelta
from dateutil import tz

import artificial_stream_generators
from DaQMeasuresFactory import DaQMeasuresFactory as daq


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WINDOW_DURATION_SEC = 20
WAIT_FOR_DELAYED_SEC = 1
SINK_FILE_NAME = "statistics_manager_sink.csv"

data = (artificial_stream_generators.generate_artificial_random_viewership_data_stream(number_of_rows=10, input_rate=1)
        .with_columns(date_and_time=pw.this.timestamp.dt.strptime(TIME_FORMAT))
        )

data = data.windowby(
    data.date_and_time,
    # data.timestamp,
    window=pw.temporal.tumbling(duration=timedelta(seconds=WINDOW_DURATION_SEC)),
    # window=pw.temporal.tumbling(duration=WINDOW_DURATION_SEC),
    instance=data.user_id,
    behavior=pw.temporal.exactly_once_behavior(shift=timedelta(seconds=WAIT_FOR_DELAYED_SEC)),
).reduce(
    window_start=pw.this._pw_window_start,
    window_end=pw.this._pw_window_end,
    # min=daq.get_min_reducer('interaction_events'),
    # max=daq.get_max_reducer('interaction_events'),
    # mean=daq.get_avg_reducer('interaction_events'),
    count=daq.get_count_reducer('interaction_events'),
    # ndarray=daq.get_ndarray_reducer('interaction_events'),
    tuple=daq.get_tuple_reducer('interaction_events'),
    # sorted_tuple=daq.get_sorted_tuple_reducer('interaction_events'),
    # above_mean=daq.get_number_of_values_above_mean_reducer('interaction_events'),
    # above_mean_frac=daq.get_fraction_of_values_above_mean_reducer('interaction_events'),
    # distinct=daq.get_number_of_distinct_values_reducer('interaction_events'),
    distinct_approx=daq.get_approx_number_of_distinct_values_reducer('interaction_events'),
    # distinct_frac=daq.get_fraction_of_distinct_values_reducer('interaction_events'),
    # distinct_frac_approx=daq.get_approx_fraction_of_distinct_values_reducer('interaction_events'),
    # unique=daq.get_number_of_unique_values_reducer('interaction_events'),
    # unique_frac=daq.get_fraction_of_unique_values_reducer('interaction_events'),
    # std_dev=daq.get_std_dev_reducer('interaction_events'),
    distinct_count=daq.get_hpp_reducer('interaction_events'),
)
pw.debug.compute_and_print(data, include_id=False)
# pw.io.csv.write(data, SINK_FILE_NAME)
# pw.run()
