from datetime import datetime
from typing import Tuple

import pathway as pw
from pathway import ColumnExpression


class DaQMeasures:
    @staticmethod
    def min(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a min pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the min reducer on.
        :return: a pathway min reducer
        """
        return pw.reducers.min(pw.this[column_name])

    @staticmethod
    def max(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a max pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the max reducer on.
        :return: a pathway max reducer
        """
        return pw.reducers.max(pw.this[column_name])

    @staticmethod
    def count(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a count pathway reducer, applied on current table (pw.this) in the specified column
        by column name
        :param column_name: the column name of pw.this table to apply the count reducer on.
        :return: a pathway count reducer
        """
        return pw.reducers.count(pw.this[column_name])

    @staticmethod
    def availability(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a count pathway reducer, applied on current table (pw.this) in the specified column
        by column name
        :param column_name: the column name of pw.this table to apply the availability reducer on.
        :return: a pathway count reducer
        """

        def get_availability(count: int) -> bool:
            return count > 0

        return pw.apply_with_type(get_availability, bool, pw.reducers.count(pw.this[column_name]))

    @staticmethod
    def mean(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve an mean pathway reducer, applied on current table (pw.this) in the specified column
        by column name
        :param column_name: the column name of pw.this table to apply the mean reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway mean reducer
        """
        return pw.apply_with_type(round, float, pw.reducers.avg(pw.this[column_name]), precision)

    @staticmethod
    def median(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median pathway reducer, applied on current table (pw.this) in the specified column
        by column name
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :return: a pathway median reducer
        """
        from streamdaq.utils import calculate_median

        return pw.apply(calculate_median, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def min_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a min length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the min reducer on.
        :return: a pathway min reducer
        """

        def get_min_length(elements):
            from streamdaq.utils import map_to_length

            lengths = map_to_length(elements)
            return min(lengths)

        return pw.apply_with_type(get_min_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def max_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a max length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the max reducer on.
        :return: a pathway max integer part length reducer
        """

        def get_max_length(elements):
            from streamdaq.utils import map_to_length

            lengths = map_to_length(elements)
            return max(lengths)

        return pw.apply_with_type(get_max_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def mean_length(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a mean length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the mean reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway mean length reducer
        """

        def get_mean_length(elements):
            from streamdaq.utils import calculate_fraction, map_to_length

            lengths = map_to_length(elements)
            return calculate_fraction(sum(lengths), len(lengths), precision)

        return pw.apply_with_type(get_mean_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def median_length(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median length pathway reducer, applied on current table (pw.this) and
        in the column specified by column name.
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :return: a pathway median length reducer
        """

        def get_median_length(elements):
            from streamdaq.utils import calculate_median, map_to_length

            lengths = map_to_length(elements)
            return calculate_median(lengths)

        return pw.apply_with_type(get_median_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def min_integer_part_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a min integer part length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the min reducer on.
        :return: a pathway min reducer
        """

        def get_min_integer_part_length(numbers):
            from streamdaq.utils import map_to_digit_count_in_integer_part

            integer_part_lengths = map_to_digit_count_in_integer_part(numbers)
            return min(integer_part_lengths)

        return pw.apply_with_type(get_min_integer_part_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def max_integer_part_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a max integer part length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the max reducer on.
        :return: a pathway max integer part length reducer
        """

        def get_max_integer_part_length(numbers):
            from streamdaq.utils import map_to_digit_count_in_integer_part

            integer_part_lengths = map_to_digit_count_in_integer_part(numbers)
            return max(integer_part_lengths)

        return pw.apply_with_type(get_max_integer_part_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def mean_integer_part_length(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a mean integer part length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the mean reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway mean integer part length reducer
        """

        def get_mean_integer_part_length(numbers):
            from streamdaq.utils import calculate_fraction, map_to_digit_count_in_integer_part

            integer_part_lengths = map_to_digit_count_in_integer_part(numbers)
            return calculate_fraction(sum(integer_part_lengths), len(integer_part_lengths), precision)

        return pw.apply_with_type(get_mean_integer_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def median_integer_part_length(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median integer part length pathway reducer, applied on current table (pw.this) and
        in the column specified by column name.
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :return: a pathway median integer part length reducer
        """

        def get_median_integer_part_length(numbers):
            from streamdaq.utils import calculate_median, map_to_digit_count_in_integer_part

            integer_part_lengths = map_to_digit_count_in_integer_part(numbers)
            return calculate_median(integer_part_lengths)

        return pw.apply_with_type(get_median_integer_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def min_fractional_part_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a min fractional part length pathway reducer, applied on current table (pw.this) in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the min reducer on.
        :return: a pathway min reducer
        """

        def get_min_fractional_part_length(numbers):
            from streamdaq.utils import map_to_digit_count_in_fractional_part

            fractional_part_lengths = map_to_digit_count_in_fractional_part(numbers)
            return min(fractional_part_lengths)

        return pw.apply_with_type(get_min_fractional_part_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def max_fractional_part_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a max fractional part length pathway reducer, applied on current table (pw.this) in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the max reducer on.
        :return: a pathway max fractional part length reducer
        """

        def get_max_fractional_part_length(numbers):
            from streamdaq.utils import map_to_digit_count_in_fractional_part

            fractional_part_lengths = map_to_digit_count_in_fractional_part(numbers)
            return max(fractional_part_lengths)

        return pw.apply_with_type(get_max_fractional_part_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def mean_fractional_part_length(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a mean fractional part length pathway reducer, applied on current table (pw.this) in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the mean reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway mean fractional part length reducer
        """

        def get_mean_fractional_part_length(numbers):
            from streamdaq.utils import calculate_fraction, map_to_digit_count_in_fractional_part

            fractional_part_lengths = map_to_digit_count_in_fractional_part(numbers)
            return calculate_fraction(sum(fractional_part_lengths), len(fractional_part_lengths), precision)

        return pw.apply_with_type(get_mean_fractional_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def median_fractional_part_length(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median fractional part length pathway reducer, applied on current table (pw.this)
        in the column specified by column name.
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :return: a pathway median fractional part length reducer
        """

        def get_median_fractional_part_length(numbers):
            from streamdaq.utils import calculate_median, map_to_digit_count_in_fractional_part

            fractional_part_lengths = map_to_digit_count_in_fractional_part(numbers)
            return calculate_median(fractional_part_lengths)

        return pw.apply_with_type(get_median_fractional_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def missing_count(column_name: str, disguised: set | None = None) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of missing values in the specified column
        of the current table (pw.this). The missing values are considered to be either empty strings or None values by
        default, but you can specify a set of disguised missing values that will also be considered as missing.
        For example, you may want to treat 9999 or -1 as (disguised) missing values. If so, please provide these values
        as a set to the ``disguised`` argument.
        :param column_name: the column name of pw.this table to apply the reducer on.
        :param disguised: a set of disguised missing values to consider as missing. Defaults to None.
        :return: a pathway pw.apply statement ready for use as a column.
        """
        from streamdaq.utils import calculate_set_conformance_count

        explicit = {"", None}  # explicit missing values are considered empty strings or None values
        missing_values = explicit if not disguised else explicit.union(disguised)

        return pw.apply(calculate_set_conformance_count, pw.reducers.tuple(pw.this[column_name]), missing_values)

    @staticmethod
    def missing_fraction(
        column_name: str, disguised: set | None = None, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of missing values in the specified column
        of the current table (pw.this). The missing values are considered to be either empty strings or None values by
        default, but you can specify a set of disguised missing values that will also be considered as missing.
        For example, you may want to treat 9999 or -1 as (disguised) missing values. If so, please provide these values
        as a set to the ``disguised`` argument. The fraction is in range [0, 1] and is rounded to the specified
        precision. The precision is the number of decimal points to include in the result. Defaults to 3.
        :param column_name: the column name of pw.this table to apply the reducer on.
        :param disguised: the set of disguised missing values to consider as missing. Defaults to None.
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column.
        """
        explicit = {"", None}  # explicit missing values are considered empty strings or None values
        missing_values = explicit if not disguised else explicit.union(disguised)

        def get_set_conformance_fraction(elements: tuple):
            from streamdaq.utils import calculate_set_conformance_count

            fraction = calculate_set_conformance_count(elements, missing_values) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_set_conformance_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def is_frozen(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes whether all the values inside the window are the same
        or not. The result is a boolean variable (True/False).
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.CustomReducers import all_values_the_same_reducer

        return all_values_the_same_reducer(pw.this[column_name])

    @staticmethod
    def satisfies_ordering(
        column_name: str, time_column: str, time_format: str, ordering: str
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that checks conformance of the values to the specified ordering,
        applied on the specified column of the current table (pw.this). The elements are first sorted in chronological
        order, so the name of the column that contains timestamps is required, as well as the time format these values
        have. The ordering can be one of the following options and refer to the ordering of values **after being sorted
        in chronological order**: \n
        - ``"ASC"``: values are in strictly ascending order \n
        - ``"ASC_EQ"``: values are in ascending order (every next element is greater **or equal** to the previous one)
        \n
        - ``"DESC"``: values are in strictly descending order \n
        - ``"DESC_EQ"``: values are in descending order (every next element is smaller **or equal** to the previous one)
        \n
        :param time_column: the column name of pw.this table that contains timestamps
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :param time_format: the format of the timestamps in ``time_column``
        :param ordering: the ordering to check for. Available options ``"ASC"``, ``"ASC_EQ"``, ``"DESC"``,
        ``"DESC_EQ"``.
        :return: a pathway custom reducer that checks conformance of the values to the specified ordering
        """

        def sort_by_time_and_check_ordering(timestamps: list, elements: list, time_format: str, ordering: str) -> bool:
            from streamdaq.utils import elements_satisfy_ordering, sort_by_timestamp

            sorted_timestamps, sorted_elements = sort_by_timestamp(timestamps, elements, time_format)
            return elements_satisfy_ordering(sorted_elements, ordering)

        return pw.apply(
            sort_by_time_and_check_ordering,
            pw.reducers.tuple(pw.this[time_column]),
            pw.reducers.tuple(pw.this[column_name]),
            time_format,
            ordering,
        )

    @staticmethod
    def most_frequent(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a most-frequent-element reducer, applied on current table (pw.this) and in the column
        specified by column name argument.
        :param column_name: the column name of @code{pw.this} table to apply the reducer on
        :return: a pathway @code{pw.apply} statement ready for use as a column
        """

        def get_most_frequent_element(elements: tuple):
            from streamdaq.utils import get_most_frequent_element

            (most_frequent_element, max_frequency) = get_most_frequent_element(elements)
            return most_frequent_element

        return pw.apply(get_most_frequent_element, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def constancy(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        **Constancy is the frequency of the most frequent element.**

        Static getter to retrieve a constancy reducer, applied on current table (pw.this) and in the column
        specified by column name argument.
        :param column_name: the column name of @code{pw.this} table to apply the reducer on
        :return: a pathway @code{pw.apply} statement ready for use as a column
        """

        def get_constancy(elements: tuple):
            from streamdaq.utils import get_most_frequent_element

            (most_frequent_element, max_frequency) = get_most_frequent_element(elements)
            return max_frequency

        return pw.apply(get_constancy, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def most_frequent_approx(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the standard deviation of the values in the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.CustomReducers import approx_most_frequent_items_reducer

        return approx_most_frequent_items_reducer(pw.this[column_name])

    @staticmethod
    def window_duration() -> datetime:
        """
        Static getter to retrieve the duration of the window. Semantically meaningful only on session- or count-based
        windows.
        :return: a datetime object representing the duration of the window
        """
        return pw.this._pw_window_end - pw.this._pw_window_start

    @staticmethod
    def ndarray(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve an ndarray pathway reducer, applied on current table (pw.this) in the specified column
        by column name. **There is no guarantee that the items in the array will be ordered in chronological event time
        order.** In case your application requires ensured chronological ordering, please consider using
        ``tuple_sorted_by_time`` measure.
        :param column_name: the column name of pw.this table to apply the ndarray reducer on.
        :return: a pathway ndarray reducer
        """
        return pw.reducers.ndarray(pw.this[column_name])

    @staticmethod
    def tuple(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a tuple pathway reducer, applied on current table (pw.this) in the specified column
        by column name
        :param column_name: the column name of pw.this table to apply the tuple reducer on.
        :return: a pathway tuple reducer
        """
        return pw.reducers.tuple(pw.this[column_name])

    @staticmethod
    def tuple_sorted(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a sorted_tuple pathway reducer, applied on current table (pw.this) in the column
        specified by column name
        :param column_name: the column name of pw.this table to apply the sorted_tuple reducer on.
        :return: a pathway sorted_tuple reducer
        """
        return pw.reducers.sorted_tuple(pw.this[column_name])

    @staticmethod
    def tuple_sorted_by_time(
        time_column: str, column_name: str, time_format: str
    ) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a reducer that sorts elements by timestamp, applied on current table (pw.this)
        and in the column specified by column name
        :param time_column: the column name of pw.this table
        :param column_name: the column name of pw.this table to apply the sorted_tuple reducer on.
        :param time_format:
        :return: a pathway sorted_tuple reducer
        """

        def get_sorted_elements_by_time(timestamps, elements, fmt):
            from streamdaq.utils import sort_by_timestamp

            sorted_timestamps, sorted_elements = sort_by_timestamp(timestamps, elements, fmt)
            return sorted_elements

        return pw.apply(
            get_sorted_elements_by_time,
            pw.reducers.tuple(pw.this[time_column]),
            pw.reducers.tuple(pw.this[column_name]),
            time_format,
        )

    @staticmethod
    def above_mean_count(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of elements in the window that are greater
        than the mean value of the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """
        import numpy as np

        def get_above_mean_count(numbers: list):
            mean = np.mean(numbers)
            return (numbers > mean).sum()

        return pw.apply(get_above_mean_count, pw.reducers.ndarray(pw.this[column_name]))

    @staticmethod
    def above_mean_fraction(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of elements in the window that are greater
        than the mean value of the window. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """
        import numpy as np

        def get_above_mean_fraction(numbers: list):
            mean = np.mean(numbers)
            fraction_above_mean = float((numbers > mean).sum() / len(numbers))
            return round(fraction_above_mean, precision)

        return pw.apply(get_above_mean_fraction, pw.reducers.ndarray(pw.this[column_name]))

    @staticmethod
    def distinct_count(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of distinct elements in the window.
        The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_distinct_count(numbers: list):
            return len(set(numbers))

        return pw.apply(get_distinct_count, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def distinct_fraction(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of distinct elements in the window.
        The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_distinct_fraction(numbers: list):
            fraction = len(set(numbers)) / len(numbers)
            return round(fraction, precision)

        return pw.apply(get_distinct_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def distinct_placeholder_count(column_name: str, placeholders: set) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of distinct placeholder values in the
        specified column of the current table (pw.this). The placeholders are specified as a set of values.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param placeholders: a set of placeholder values to count in the specified column
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_distinct_placeholder_count(elements: tuple) -> int:
            """
            Counts the number of distinct placeholder values in the provided elements.
            :param elements: list of elements to check for placeholders
            :return: count of distinct placeholders
            """
            return len(set(elements).intersection(placeholders))

        return pw.apply(get_distinct_placeholder_count, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def distinct_placeholder_fraction(
        column_name: str, placeholders: set, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of distinct placeholder values in the
        specified column of the current table (pw.this). The placeholders are specified as a set of values.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param placeholders: a set of placeholder values to count in the specified column
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_distinct_placeholder_fraction(elements: tuple) -> float:
            """
            Computes the fraction of distinct placeholder values in the provided elements.
            :param elements: list of elements to check for placeholders
            :return: fraction of distinct placeholders
            """
            distinct_count = len(set(elements).intersection(placeholders))
            return round(distinct_count / len(elements), precision)

        return pw.apply(get_distinct_placeholder_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def distinct_count_approx(column_name: str, precision: int = 0) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the approximate number of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result.
        Defaults to 0 (round to integer).
        :return: a pathway pw.apply statement ready for use as a column
        """
        from streamdaq.CustomReducers import approx_distinct_count_reducer

        return pw.apply(round, approx_distinct_count_reducer(pw.this[column_name]), precision)

    @staticmethod
    def distinct_fraction_approx(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the approximate fraction of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """
        from streamdaq.CustomReducers import approx_distinct_count_reducer

        def get_distinct_fraction_approx(distinct_count: float, total_count: int) -> float:
            from streamdaq.utils import calculate_fraction

            return calculate_fraction(distinct_count, total_count, precision)

        return pw.apply(
            get_distinct_fraction_approx,
            approx_distinct_count_reducer(pw.this[column_name]),
            pw.reducers.count(pw.this[column_name]),
        )

    @staticmethod
    def unique_count(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of unique elements in the window.
        The fraction is in range [0, 1].
        IMPORTANT: Unique values are considered the ones that appear **exactly** once inside the window. For example, in
        [a, a, b] the only unique value is 'b'. In case you wish 'a' and 'b' to appear in the result, consider using
        a reducer calculating **distinct** values, instead.
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_unique_count(elements: list):
            from streamdaq.utils import calculate_number_of_unique_values

            return calculate_number_of_unique_values(elements)

        return pw.apply(get_unique_count, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def unique_fraction(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of unique elements in the window.
        The fraction is in range [0, 1].
        IMPORTANT: Unique values are considered the ones that appear **exactly** once inside the window. For example, in
        [a, a, b] the only unique value is 'b'. In case you wish 'a' and 'b' to appear in the result, consider using
        a reducer calculating **distinct** values, instead.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_unique_fraction(elements: list) -> float:
            from streamdaq.utils import calculate_number_of_unique_values

            fraction = calculate_number_of_unique_values(elements) / len(elements)
            return round(fraction, precision)

        return pw.apply_with_type(get_unique_fraction, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def unique_over_distinct_fraction(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of unique elements over distinct ones
        in the window. The fraction is in range [0, 1], since #unique <= # distinct elements and the equality holds only
        for the case where all values inside a window are different from one another.
        DEFINITION: Unique values are considered the ones that appear **exactly** once inside the window. For example,
        in [a, a, b] the only unique value is 'b'. Distinct values are considered the ones that appear **at least** once
        inside the window. For example, in [a, a, b] the distinct values are 'a' and 'b'.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_unique_over_distinct_fraction(elements: list):
            from streamdaq.utils import calculate_fraction, calculate_number_of_unique_values

            return calculate_fraction(calculate_number_of_unique_values(elements), len(set(elements)), precision)

            # fraction = calculate_number_of_unique_values(elements) / len(set(elements))
            # return round(fraction, precision)

        return pw.apply(get_unique_over_distinct_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def std_dev(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the standard deviation of the values in the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.CustomReducers import std_dev_reducer

        return pw.apply(round, std_dev_reducer(pw.this[column_name]), precision)

    @staticmethod
    def range_conformance_count(
        column_name: str, low: float, high: float, inclusive: bool | Tuple[bool] = True
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of values in the window that fall
        within the range specified by low and high arguments. If the inclusive argument is set to True, the range
        of allowed values is [low, high], otherwise (low, high). The default behavior is inclusive range.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param low: the lower bound of the range
        :param high: the upper bound of the range
        :param inclusive: whether to include or not the bounds in the allowed range. Defaults to True, thus [low, high].
                          In case inclusive is a tuple, it should contain two boolean values,
                          the first one indicating whether to include the lower bound and the second one indicating
                          whether to include the upper bound. For example, (True, False) means [low, high).
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_range_conformance_count

        return pw.apply(
            calculate_range_conformance_count, pw.reducers.tuple(pw.this[column_name]), low, high, inclusive
        )

    @staticmethod
    def range_conformance_fraction(
        column_name: str, low: float, high: float, inclusive=True, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of values in the window that fall
        within the range specified by low and high arguments. If the inclusive argument is set to True, the range
        of allowed values is [low, high], otherwise (low, high). The default behavior is inclusive range. The fraction
        is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param low: the lower bound of the range
        :param high: the upper bound of the range
        :param inclusive: whether to include or not the bounds in the allowed range. Defaults to True, thus [low, high].
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_range_conformance_fraction(elements: tuple):
            from streamdaq.utils import calculate_range_conformance_count

            fraction = calculate_range_conformance_count(elements, low, high, inclusive) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_range_conformance_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def set_conformance_count(column_name: str, allowed_values: set) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of values in the window that are contained
        in the specified set of allowed values.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_set_conformance_count

        return pw.apply(calculate_set_conformance_count, pw.reducers.tuple(pw.this[column_name]), allowed_values)

    @staticmethod
    def set_conformance_fraction(
        column_name: str, allowed_values: set, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of values in the window that are contained
        in the specified set of allowed values. The fraction is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_set_conformance_fraction(elements: tuple):
            from streamdaq.utils import calculate_set_conformance_count

            fraction = calculate_set_conformance_count(elements, allowed_values) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_set_conformance_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def percentiles(
        column_name: str, percentiles: int | list = [25, 50, 75], precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the specified percentiles of the values in the window.
        If no percentiles are specified, the default ones are 25, 50 and 75. The percentiles argument can be a single
        integer or a list of integers, representing the percentiles to compute. The precision argument specifies the
        number of decimal points to include in the result. In case multiple percentiles are given, all percentile values
        are rounded to the same number of decimal points.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param percentiles: a single (int) or multiple percentiles (list of int) to compute
        :param precision: the number of decimal points to include in the result. Defaults to 3. In case multiple
        percentiles are given, all percentile values are rounded to the same number of decimal points.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import get_percentiles

        """
        IMPORTANT: percentiles argument has to be passed to pw.apply either as a tuple or a numpy.ndarray. Tuple was
        preferred in this case due to performance reasons, since importing numpy and using an ndarray would be
        a clear overkill.
        The argument cannot be passed as a normal python list, since pathway has a predefined set of datatypes that
        can be passed as arguments to the pw.apply function. Python list is not included in that list. numpy.ndarrays
        and tuples are both included, among others.
        """
        return pw.apply(get_percentiles, pw.reducers.ndarray(pw.this[column_name]), tuple(percentiles), precision)

    @staticmethod
    def first_digit_frequencies(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the specified percentiles of the values in the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the result. Defaults to 3. In case multiple
        percentiles are given, all percentile values are rounded to the same number of decimal points.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        from streamdaq.utils import get_first_digit_frequencies

        return pw.apply(get_first_digit_frequencies, pw.reducers.ndarray(pw.this[column_name]), precision)

    @staticmethod
    def range_conformance_count_only_most_frequent(
        column_name: str, low: float, high: float, inclusive=True
    ) -> pw.internals.expression.ColumnExpression:
        # todo: potentially this can be merged (with one more parameter) with range_conformance_count
        """
        Static getter to retrieve a custom reducer that computes the number of **the most frequent elements** in the
        window that fall within the range specified by low and high arguments. If the inclusive argument is set to True,
        the range of allowed values is [low, high], otherwise (low, high). The default behavior is inclusive range.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param low: the lower bound of the range
        :param high: the upper bound of the range
        :param inclusive: whether to include or not the bounds in the allowed range. Defaults to True, thus [low, high].
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_range_conformance_count

        return pw.apply(calculate_range_conformance_count, DaQMeasures.most_frequent(column_name), low, high, inclusive)

    @staticmethod
    def range_conformance_fraction_only_most_frequent(
        column_name: str, low: float, high: float, inclusive=True, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        # todo: potentially this can be merged (with one more parameter) with range_conformance_fraction
        """
        Static getter to retrieve a custom reducer that computes the fraction of **the most frequent elements** in the
        window that fall within the range specified by low and high arguments. If the inclusive argument is set to True,
        the range of allowed values is [low, high], otherwise (low, high). The default behavior is inclusive range.
        The fraction is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param low: the lower bound of the range
        :param high: the upper bound of the range
        :param inclusive: whether to include or not the bounds in the allowed range. Defaults to True, thus [low, high].
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_fraction_of_most_frequent_range_conformance(elements: tuple):
            from streamdaq.utils import calculate_range_conformance_count

            try:
                length = len(elements)
            except TypeError:
                length = 1
            fraction = calculate_range_conformance_count(elements, low, high, inclusive) / length
            return round(fraction, precision)

        return pw.apply(get_fraction_of_most_frequent_range_conformance, DaQMeasures.most_frequent(column_name))

    @staticmethod
    def set_conformance_count_only_most_frequent(
        column_name: str, allowed_values: set
    ) -> pw.internals.expression.ColumnExpression:
        # todo: potentially this can be merged (with one more parameter) with set_conformance_count
        """
        Static getter to retrieve a custom reducer that computes the number of **the most frequent elements** in the
        window that are contained in the specified set of allowed values.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_set_conformance_count

        return pw.apply(calculate_set_conformance_count, DaQMeasures.most_frequent(column_name), allowed_values)

    @staticmethod
    def set_conformance_fraction_only_most_frequent(
        column_name: str, allowed_values: set, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        # todo: potentially this can be merged (with one more parameter) with set_conformance_fraction
        """
        Static getter to retrieve a custom reducer that computes the fraction of **the most frequent elements** in the
        window that are contained in the specified set of allowed values. The fraction is a float in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_fraction_of_most_frequent_set_conformance(elements: tuple):
            from streamdaq.utils import calculate_set_conformance_count

            try:
                iter(elements)
            except TypeError:
                elements = [elements]

            fraction = calculate_set_conformance_count(elements, allowed_values) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_most_frequent_set_conformance, DaQMeasures.most_frequent(column_name))

    @staticmethod
    def regex_conformance_count(column_name: str, regex: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of values in the window that match the
        specified regex argument. The provided ``regex`` argument has to comply with the built-in python library ``re``.
        (https://docs.python.org/3/library/re.html).
        :param column_name: the column name of pw.this table to apply the reducer on
        :param regex: the regex to check the values for matching. Has to comply with the built-in python library ``re``.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_regex_conformance_count

        return pw.apply(calculate_regex_conformance_count, pw.reducers.tuple(pw.this[column_name]), regex)

    @staticmethod
    def regex_conformance_fraction(
        column_name: str, regex: str, precision: int = 3
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of values in the window that match the
        specified regex argument. The provided ``regex`` argument has to comply with the built-in python library ``re``.
        (https://docs.python.org/3/library/re.html). The fraction is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param regex: the regex to check the values for matching. Has to comply with the built-in python library ``re``.
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_regex_conformance_fraction(elements: tuple):
            from streamdaq.utils import calculate_regex_conformance_count

            fraction = calculate_regex_conformance_count(elements, regex) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_regex_conformance_fraction, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def correlation(
        first_column_name: str, second_column_name: str, precision: int = 3, method: str = "pearson"
    ) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the correlation or association between two
        columns over a window, using a selected method. Supports Pearson, Spearman, Kendall correlations
        and Cramer's V assosiation.

        Note:
            - Correlation measures in what way two variables are related, whereas, association measures how related
              the variables are.
            - Cramér's V supports only non-negative integer numbers. If the input contains negative or float values,
              the result will be NaN.

        Internally uses the following implementations:
            - Pearson correlation: `scipy.stats.pearsonr
              <https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html>`_
            - Spearman correlation: `scipy.stats.spearmanr
              <https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.spearmanr.html>`_
            - Kendall tau: `scipy.stats.kendalltau
              <https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kendalltau.html>`_
            - Cramér's V: `scipy.stats.contingency.association
              <https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.contingency.association.html>`_

        :param first_column_name: The name of the first (x) column.
        :type first_column_name: str
        :param second_column_name: The name of the second (y) column.
        :type second_column_name: str
        :param precision: Number of decimal points to round the result to. Defaults to 3.
        :type precision: int, optional
        :param method: The correlation/association method to use. Must be one of:
                      'pearson', 'spearman', 'kendall', or 'cramer'.
        :type method: str
        :return: A `pw.ColumnExpression` representing the result of applying the reducer
                 to the specified columns.
        :rtype: pw.ColumnExpression
        :raises ValueError: If an unsupported correlation method is provided.

        Examples:
            Define a simple pandas DataFrame with two numeric columns (colA, colB) and a timestamp column,
            then convert it into a Pathway table for use in data quality measurements. \n
            >>> df = pd.DataFrame({
            ...     "colA": [1, 1, 2, 4, 4, 7, 8, 5, 2, 6],
            ...     "colB": [4, 7, 3, 5, 6, 1, 3, 9, 1, 6],
            ...     "timestamp": [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]})
            >>> t = pw.debug.table_from_pandas(df)

            Add Pearson's correlation as a data quality measurement.

            >>> daq.add(dqm.correlation("colA", "colB", precision=3, method="pearson"),
            ...          assess=">0.1", name="Pearson's Correlation")

            Output

            .. table::
            ==============  ===========  ======================
            window_start    window_end   Pearson's Correlation
            ==============  ===========  ======================
            10              15           (0.1043, True)
            15              20           (0.0627, False)
            ==============  ===========  ======================

            Add Spearman's correlation as a data quality measurement.

            >>> daq.add(dqm.correlation("colA", "colB", precision=3, method="spearman"),
            ...          assess=">0.5", name="Spearman's Correlation")

            Output

            .. table::
            ==============  ===========  ======================
            window_start    window_end   Spearman's Correlation
            ==============  ===========  ======================
            10              15           (0.0, False)
            15              20           (-0.0513, False)
            ==============  ===========  ======================

            Add Kendall's tau as a data quality measurement.

            >>> daq.add(dqm.correlation("colA", "colB", precision=3, method="kendall"),
            ...          assess=">-0.1", name="Kendall's tau")

             Output

            .. table::
            ==============  ===========  ======================
            window_start    window_end   Kendall's tau
            ==============  ===========  ======================
            10              15           (0.0, True)
            15              20           (-0.1054, False)
            ==============  ===========  ======================

            Add Cramér's V as a data quality measurement.

            >>> daq.add(dqm.correlation("colA", "colB", precision=3, method="cramer"), assess=">0.2", name="Cramer's V")

            Output

            .. table::
            ==============  ===========  ======================
            window_start    window_end   Cramer's V
            ==============  ===========  ======================
            10              15           (0.2745, True)
            15              20           (0.385, True)
            ==============  ===========  ======================

        """
        from streamdaq.utils import calculate_correlation

        x = pw.reducers.ndarray(pw.this[first_column_name])
        y = pw.reducers.ndarray(pw.this[second_column_name])

        # Supported correlation/association methods
        supported_methods = ["pearson", "spearman", "kendall", "cramer"]

        if method not in supported_methods:
            raise ValueError(f"Unsupported correlation/association method: {method}. Choose from {supported_methods}.")

        return pw.apply(calculate_correlation, x, y, precision, method)

    @staticmethod
    def trend(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a trend analysis reducer that computes the slope of the linear trend
        in the values within a window. The trend is calculated using linear regression, where the
        x-values are sequential indices (0, 1, 2, ...) and y-values are the column values.

        This measure helps identify if values are generally increasing (positive slope),
        decreasing (negative slope), or staying flat (slope near zero) over time within the window.

        :param column_name: The column name of pw.this table to apply the trend analysis on.
        :type column_name: str
        :param precision: Number of decimal points to round the result to. Defaults to 3.
        :type precision: int, optional
        :return: A `pw.ColumnExpression` representing the slope/trend of the values.
                 Positive values indicate increasing trend, negative values indicate decreasing trend,
                 and values near zero indicate flat/stable trend.
        :rtype: pw.ColumnExpression

        Examples:
            Add trend analysis as a data quality measurement to detect increasing trends.

            >>> daq.add(dqm.trend("sensor_reading"), assess=">0.1", name="increasing_trend")

            Output for an increasing trend:

            .. table::
            ==============  ===========  =================
            window_start    window_end   increasing_trend
            ==============  ===========  =================
            10              15           (0.4, True)
            15              20           (0.6, True)
            ==============  ===========  =================

            Add trend analysis to detect decreasing trends with negative slope threshold.

            >>> daq.add(dqm.trend("temperature"), assess="<-0.2", name="cooling_trend")

            Output for a decreasing trend:

            .. table::
            ==============  ===========  =================
            window_start    window_end   cooling_trend
            ==============  ===========  =================
            10              15           (-0.3, True)
            15              20           (-0.1, False)
            ==============  ===========  =================

            Add trend analysis to detect stable values (trend near zero).

            >>> daq.add(dqm.trend("stable_metric"), assess="[-0.1, 0.1]", name="stability_check")

            Output for stable values:

            .. table::
            ==============  ===========  =================
            window_start    window_end   stability_check
            ==============  ===========  =================
            10              15           (0.05, True)
            15              20           (0.15, False)
            ==============  ===========  =================

        """
        from streamdaq.utils import calculate_trend

        return pw.apply(calculate_trend, pw.reducers.tuple(pw.this[column_name]), precision)
