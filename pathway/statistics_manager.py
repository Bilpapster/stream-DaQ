import pathway as pw
from datetime import timedelta

import artificial_stream_generators
from DaQMeasures import DaQMeasures as measures

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
WINDOW_DURATION_SEC = 22
WAIT_FOR_DELAYED_SEC = 1
SINK_FILE_NAME = "statistics_manager_sink.csv"
ALLOWED_VALUES = (2, 4, 6, 8)

data = (artificial_stream_generators.generate_artificial_random_viewership_data_stream(number_of_rows=20, input_rate=1)
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
    duration=measures.window_duration(),
    min=measures.min('interaction_events'),
    max=measures.max('interaction_events'),
    # mean=measures.mean('interaction_events'),
    # median=measures.median('interaction_events'),
    # min_len=measures.min_length('languages'),
    # max_len=measures.max_length('languages'),
    # mean_len=measures.mean_length('languages'),
    # median_len=measures.median_length('languages'),
    # min_int_part_len=measures.min_integer_part_length('duration_watched'),
    # max_int_part_len=measures.max_integer_part_length('duration_watched'),
    # mean_int_part_len=measures.mean_integer_part_length('duration_watched'),
    # median_int_part_len=measures.median_integer_part_length('duration_watched'),
    # min_frac_part_len=measures.min_fractional_part_length('duration_watched'),
    # max_frac_part_len=measures.max_fractional_part_length('duration_watched'),
    # mean_frac_part_len=measures.mean_fractional_part_length('duration_watched'),
    # median_frac_part_len=measures.median_fractional_part_length('duration_watched'),
    # all_same=measures.same_values('interaction_events'),
    # asc=measures.ordering('date_and_time', 'interaction_events', TIME_FORMAT, "ASC"),
    # asc_eq=measures.ordering('date_and_time', 'interaction_events', TIME_FORMAT, "ASC_EQ"),
    # desc=measures.ordering('date_and_time', 'interaction_events', TIME_FORMAT, "DESC"),
    # desc_eq=measures.ordering('date_and_time', 'interaction_events', TIME_FORMAT, "DESC_EQ"),
    # most_frequent=measures.most_frequent('interaction_events'),
    # frequent_items_approx=measures.most_frequent_approx('interaction_events'),
    # constancy=measures.constancy('interaction_events'),
    # count=measures.count('interaction_events'),
    # availability=measures.availability('interaction_events'),
    # ndarray=measures.ndarray('interaction_events'),
    # tuple=measures.tuple('interaction_events'),
    # sorted_tuple=measures.tuple_sorted('interaction_events'),
    # sorted_by_time=measures.tuple_sorted_by_time('date_and_time', 'interaction_events', TIME_FORMAT),
    # above_mean=measures.number_above_mean('interaction_events'),
    # above_mean_frac=measures.fraction_above_mean('interaction_events'),
    # distinct=measures.number_of_distinct('interaction_events'),
    # distinct_approx=measures.number_of_distinct_approx('interaction_events'),
    # distinct_frac=measures.fraction_of_distinct('interaction_events'),
    # distinct_frac_approx=measures.fraction_of_distinct_approx('interaction_events'),
    # unique=measures.number_of_unique('interaction_events'),
    # unique_frac=measures.fraction_of_unique('interaction_events'),
    # unique_over_distinct_frac=measures.fraction_of_unique_over_distinct('interaction_events'),
    # std_dev=measures.std_dev('interaction_events'),
    # percentiles=measures.percentiles('interaction_events', [10, 20, 80]),
    # range_conformance=measures.number_of_range_conformance('interaction_events', 3, 6, True),
    # most_freq_range_conformance=measures.number_of_most_frequent_range_conformance('interaction_events', 3,6, True),
    # range_conformance_frac=measures.fraction_of_range_conformance('interaction_events', 3, 6, True),
    # most_freq_range_conformance_frac=measures.fraction_of_most_frequent_range_conformance('interaction_events', 3, 6, True),
    # set_conformance=measures.number_of_set_conformance('interaction_events', ALLOWED_VALUES),
    # most_frequent_set_conformance=measures.number_of_most_frequent_set_conformance('interaction_events', ALLOWED_VALUES),
    # set_conformance_frac=measures.fraction_of_set_conformance('interaction_events', ALLOWED_VALUES),
    # most_frequent_set_conformance_frac=measures.fraction_of_most_frequent_set_conformance('interaction_events', ALLOWED_VALUES),
    # tuple_lang=measures.tuple('languages'),
    # regex=measures.number_of_regex_conformance('languages', r'.*ish'),
    # regex_frac=measures.fraction_of_regex_conformance('languages', r'.*ish'),
    # first_digit_frequencies=measures.get_first_digit_frequencies_reducer('interaction_events'),
    pearson=measures.pearson('interaction_events', 'duration_watched'),
)
pw.debug.compute_and_print(data, include_id=False)
# pw.io.csv.write(data, SINK_FILE_NAME)
# pw.run()
