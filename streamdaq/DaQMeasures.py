import pathway as pw
from datetime import datetime

from pathway import ColumnExpression

from streamdaq.utils import Regex

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
        Static getter to retrieve a count pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the count reducer on.
        :return: a pathway count reducer
        """
        return pw.reducers.count(pw.this[column_name])

    @staticmethod
    def availability(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a count pathway reducer, applied on current table (pw.this) and in the column specified
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
        Static getter to retrieve an mean pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the mean reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway mean reducer
        """
        return pw.apply_with_type(round, float, pw.reducers.avg(pw.this[column_name]), precision)

    @staticmethod
    def median(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median pathway reducer, applied on current table (pw.this) and in the column specified
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
            from streamdaq.utils import map_to_length, calculate_fraction

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
            from streamdaq.utils import map_to_length, calculate_median

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
            from streamdaq.utils import compute_number_of_digits_in_integer_parts

            integer_part_lengths = compute_number_of_digits_in_integer_parts(numbers)
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
            from streamdaq.utils import compute_number_of_digits_in_integer_parts

            integer_part_lengths = compute_number_of_digits_in_integer_parts(numbers)
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
            from streamdaq.utils import compute_number_of_digits_in_integer_parts, calculate_fraction

            integer_part_lengths = compute_number_of_digits_in_integer_parts(numbers)
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
            from streamdaq.utils import compute_number_of_digits_in_integer_parts, calculate_median

            integer_part_lengths = compute_number_of_digits_in_integer_parts(numbers)
            return calculate_median(integer_part_lengths)

        return pw.apply_with_type(get_median_integer_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def min_fractional_part_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a min fractional part length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the min reducer on.
        :return: a pathway min reducer
        """

        def get_min_fractional_part_length(numbers):
            from streamdaq.utils import compute_number_of_digits_in_fractional_parts

            fractional_part_lengths = compute_number_of_digits_in_fractional_parts(numbers)
            return min(fractional_part_lengths)

        return pw.apply_with_type(get_min_fractional_part_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def max_fractional_part_length(column_name: str) -> ColumnExpression:
        """
        Static getter to retrieve a max fractional part length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the max reducer on.
        :return: a pathway max fractional part length reducer
        """

        def get_max_fractional_part_length(numbers):
            from streamdaq.utils import compute_number_of_digits_in_fractional_parts

            fractional_part_lengths = compute_number_of_digits_in_fractional_parts(numbers)
            return max(fractional_part_lengths)

        return pw.apply_with_type(get_max_fractional_part_length, int, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def mean_fractional_part_length(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a mean fractional part length pathway reducer, applied on current table (pw.this) and in
        the column specified by column name.
        :param column_name: the column name of pw.this table to apply the mean reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway mean fractional part length reducer
        """

        def get_mean_fractional_part_length(numbers):
            from streamdaq.utils import compute_number_of_digits_in_fractional_parts, calculate_fraction

            fractional_part_lengths = compute_number_of_digits_in_fractional_parts(numbers)
            return calculate_fraction(sum(fractional_part_lengths), len(fractional_part_lengths), precision)

        return pw.apply_with_type(get_mean_fractional_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def median_fractional_part_length(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median fractional part length pathway reducer, applied on current table (pw.this) and
        in the column specified by column name.
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :return: a pathway median fractional part length reducer
        """

        def get_median_fractional_part_length(numbers):
            from streamdaq.utils import compute_number_of_digits_in_fractional_parts, calculate_median

            fractional_part_lengths = compute_number_of_digits_in_fractional_parts(numbers)
            return calculate_median(fractional_part_lengths)

        return pw.apply_with_type(get_median_fractional_part_length, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def same_values(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes whether all the values inside the window are the same
        or not. The result is a boolean variable (True/False).
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.CustomReducers import all_values_the_same_reducer

        return all_values_the_same_reducer(pw.this[column_name])

    @staticmethod
    def ordering(time_column: str, column_name: str, time_format: str,
                 ordering: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that checks conformance of the values to the specified ordering,
        applied on the specified column of the current table (pw.this). The elements are first sorted in chronological
        order, so the name of the column that contains timestamps is required, as well as the time format these values
        have. The ordering can be one of the following options and refer to the ordering of values **after being sorted
        in chronological order**: \n
        - ``"ASC"``: values are in strictly ascending order \n
        - ``"ASC_EQ"``: values are in ascending order (every next element is greater **or equal** to the previous one) \n
        - ``"DESC"``: values are in strictly descending order \n
        - ``"DESC_EQ"``: values are in descending order (every next element is smaller **or equal** to the previous one) \n
        :param time_column: the column name of pw.this table that contains timestamps
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :param time_format: the format of the timestamps in ``time_column``
        :param ordering: the ordering to check for. Available options ``"ASC"``, ``"ASC_EQ"``, ``"DESC"``, ``"DESC_EQ"``.
        :return: a pathway custom reducer that checks conformance of the values to the specified ordering
        """

        def check_ordering(timestamps: list, elements: list, time_format: str,
                           ordering: str) -> bool:
            from streamdaq.utils import check_ordering, sort_by_timestamp

            sorted_timestamps, sorted_elements = sort_by_timestamp(timestamps, elements, time_format)
            return check_ordering(sorted_elements, ordering)

        return pw.apply(check_ordering, pw.reducers.tuple(pw.this[time_column]),
                        pw.reducers.tuple(pw.this[column_name]), time_format, ordering)

    @staticmethod
    def most_frequent(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a most-frequent-element reducer, applied on current table (pw.this) and in the column
        specified by column name argument.
        :param column_name: the column name of @code{pw.this} table to apply the reducer on
        :return: a pathway @code{pw.apply} statement ready for use as a column
        """

        def get_most_frequent_element(elements: tuple):
            from streamdaq.utils import find_most_frequent_element

            (most_frequent_element, max_frequency) = find_most_frequent_element(elements)
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
            from streamdaq.utils import find_most_frequent_element

            (most_frequent_element, max_frequency) = find_most_frequent_element(elements)
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
        Static getter to retrieve an ndarray pathway reducer, applied on current table (pw.this) and in the column specified
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
        Static getter to retrieve a tuple pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the tuple reducer on.
        :return: a pathway tuple reducer
        """
        return pw.reducers.tuple(pw.this[column_name])

    @staticmethod
    def tuple_sorted(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a sorted_tuple pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the sorted_tuple reducer on.
        :return: a pathway sorted_tuple reducer
        """
        return pw.reducers.sorted_tuple(pw.this[column_name])

    @staticmethod
    def tuple_sorted_by_time(time_column: str, column_name: str,
                             time_format: str) -> pw.internals.expression.ReducerExpression:
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

        return pw.apply(get_sorted_elements_by_time, pw.reducers.tuple(pw.this[time_column]),
                        pw.reducers.tuple(pw.this[column_name]), time_format)

    @staticmethod
    def number_above_mean(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of elements in the window that are greater
        than the mean value of the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """
        import numpy as np

        def get_number_of_values_above_mean(numbers: list):
            mean = np.mean(numbers)
            return (numbers > mean).sum()

        return pw.apply(get_number_of_values_above_mean, pw.reducers.ndarray(pw.this[column_name]))

    @staticmethod
    def fraction_above_mean(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of elements in the window that are greater
        than the mean value of the window. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """
        import numpy as np

        def get_fraction_of_values_above_mean(numbers: list):
            mean = np.mean(numbers)
            fraction_above_mean = float((numbers > mean).sum() / len(numbers))
            return round(fraction_above_mean, precision)

        return pw.apply(get_fraction_of_values_above_mean, pw.reducers.ndarray(pw.this[column_name]))

    @staticmethod
    def number_of_distinct(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of distinct elements in the window.
        The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_number_of_distinct_values(numbers: list):
            return len(set(numbers))

        return pw.apply(get_number_of_distinct_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def fraction_of_distinct(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of distinct elements in the window.
        The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_fraction_of_distinct_values(numbers: list):
            fraction = len(set(numbers)) / len(numbers)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_distinct_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def number_of_distinct_approx(column_name: str, precision: int = 0) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the approximate number of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 0 (round to integer).
        :return: a pathway pw.apply statement ready for use as a column
        """
        from streamdaq.CustomReducers import approx_distinct_count_reducer

        return pw.apply(round, approx_distinct_count_reducer(pw.this[column_name]), precision)

    @staticmethod
    def fraction_of_distinct_approx(column_name: str,
                                    precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the approximate fraction of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """
        from streamdaq.CustomReducers import approx_distinct_count_reducer

        def get_approx_fraction_of_distinct_values(distinct_count: float, total_count: int) -> float:
            from streamdaq.utils import calculate_fraction

            return calculate_fraction(distinct_count, total_count, precision)

        return pw.apply(get_approx_fraction_of_distinct_values, approx_distinct_count_reducer(pw.this[column_name]),
                        pw.reducers.count(pw.this[column_name]))

    @staticmethod
    def number_of_unique(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of unique elements in the window.
        The fraction is in range [0, 1].
        IMPORTANT: Unique values are considered the ones that appear **exactly** once inside the window. For example, in
        [a, a, b] the only unique value is 'b'. In case you wish 'a' and 'b' to appear in the result, consider using
        a reducer calculating **distinct** values, instead.
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_number_of_unique_values(elements: list):
            from streamdaq.utils import calculate_number_of_unique_values
            return calculate_number_of_unique_values(elements)

        return pw.apply(get_number_of_unique_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def fraction_of_unique(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
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

        def get_fraction_of_unique_values(elements: list) -> float:
            from streamdaq.utils import calculate_number_of_unique_values

            fraction = calculate_number_of_unique_values(elements) / len(elements)
            return round(fraction, precision)

        return pw.apply_with_type(get_fraction_of_unique_values, float, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def fraction_of_unique_over_distinct(column_name: str,
                                         precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of unique elements over distinct ones
        in the window. The fraction is in range [0, 1], since #unique <= # distinct elements and the equality holds only
        for the case where all values inside a window are different from one another.
        DEFINITION: Unique values are considered the ones that appear **exactly** once inside the window. For example, in
        [a, a, b] the only unique value is 'b'. Distinct values are considered the ones that appear **at least** once
        inside the window. For example, in [a, a, b] the distinct values are 'a' and 'b'.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_fraction_of_unique_over_distinct_values(elements: list):
            from streamdaq.utils import calculate_number_of_unique_values, calculate_fraction

            return calculate_fraction(
                calculate_number_of_unique_values(elements),
                len(set(elements)),
                precision
            )

            # fraction = calculate_number_of_unique_values(elements) / len(set(elements))
            # return round(fraction, precision)

        return pw.apply(get_fraction_of_unique_over_distinct_values, pw.reducers.tuple(pw.this[column_name]))

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
    def number_of_range_conformance(column_name: str, low: float, high: float,
                                    inclusive=True) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of values in the window that fall
        within the range specified by low and high arguments. If the inclusive argument is set to True, the range
        of allowed values is [low, high], otherwise (low, high). The default behavior is inclusive range.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param low: the lower bound of the range
        :param high: the upper bound of the range
        :param inclusive: whether to include or not the bounds in the allowed range. Defaults to True, thus [low, high].
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_number_of_range_conformance

        return pw.apply(calculate_number_of_range_conformance, pw.reducers.tuple(pw.this[column_name]), low, high,
                        inclusive)

    @staticmethod
    def fraction_of_range_conformance(column_name: str, low: float, high: float, inclusive=True,
                                      precision: int = 3) -> pw.internals.expression.ColumnExpression:
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

        def get_fraction_of_range_conformance(elements: tuple):
            from streamdaq.utils import calculate_number_of_range_conformance

            fraction = calculate_number_of_range_conformance(elements, low, high, inclusive) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_range_conformance, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def number_of_set_conformance(column_name: str, allowed_values: set) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of values in the window that are contained
        in the specified set of allowed values.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_number_of_set_conformance

        return pw.apply(calculate_number_of_set_conformance, pw.reducers.tuple(pw.this[column_name]), allowed_values)

    @staticmethod
    def fraction_of_set_conformance(column_name: str, allowed_values: set,
                                    precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of values in the window that are contained
        in the specified set of allowed values. The fraction is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_fraction_of_set_conformance(elements: tuple):
            from streamdaq.utils import calculate_number_of_set_conformance

            fraction = calculate_number_of_set_conformance(elements, allowed_values) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_set_conformance, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def percentiles(column_name: str, percentiles: int | list,
                    precision: int = 3) -> pw.internals.expression.ColumnExpression:
        # todo Thodoris suggested to add a quartiles reducer as well
        """
        Static getter to retrieve a custom reducer that computes the specified percentiles of the values in the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param percentiles: a single (int) or multiple percentiles (list of int) to compute
        :param precision: the number of decimal points to include in the result. Defaults to 3. In case multiple
        percentiles are given, all percentile values are rounded to the same number of decimal points.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import get_percentiles

        '''
        IMPORTANT: percentiles argument has to be passed to pw.apply either as a tuple or a numpy.ndarray. Tuple was
        preferred in this case due to performance reasons, since importing numpy and using an ndarray would be a clear
        overkill. 
        The argument cannot be passed as a normal python list, since pathway has a predefined set of datatypes that
        can be passed as arguments to the pw.apply function. Python list is not included in that list. numpy.ndarrays
        and tuples are both included, among others. 
        '''
        return pw.apply(get_percentiles, pw.reducers.ndarray(pw.this[column_name]), tuple(percentiles), precision)

    @staticmethod
    def get_first_digit_frequencies_reducer(column_name: str,
                                            precision: int = 3) -> pw.internals.expression.ColumnExpression:
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
    def number_of_most_frequent_range_conformance(column_name: str, low: float, high: float,
                                                  inclusive=True) -> pw.internals.expression.ColumnExpression:
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
        from streamdaq.utils import calculate_number_of_range_conformance

        return pw.apply(calculate_number_of_range_conformance,
                        DaQMeasures.most_frequent(column_name), low, high,
                        inclusive)

    @staticmethod
    def fraction_of_most_frequent_range_conformance(column_name: str, low: float, high: float, inclusive=True,
                                                    precision: int = 3) -> pw.internals.expression.ColumnExpression:
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
            from streamdaq.utils import calculate_number_of_range_conformance

            try:
                length = len(elements)
            except TypeError:
                length = 1
            fraction = calculate_number_of_range_conformance(elements, low, high, inclusive) / length
            return round(fraction, precision)

        return pw.apply(get_fraction_of_most_frequent_range_conformance,
                        DaQMeasures.most_frequent(column_name))

    @staticmethod
    def number_of_most_frequent_set_conformance(column_name: str,
                                                allowed_values: set) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of **the most frequent elements** in the
        window that are contained in the specified set of allowed values.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_number_of_set_conformance

        return pw.apply(calculate_number_of_set_conformance, DaQMeasures.most_frequent(column_name),
                        allowed_values)

    @staticmethod
    def fraction_of_most_frequent_set_conformance(column_name: str, allowed_values: set,
                                                  precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of **the most frequent elements** in the
        window that are contained in the specified set of allowed values. The fraction is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param allowed_values: a set of allowed values
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_fraction_of_most_frequent_set_conformance(elements: tuple):
            from streamdaq.utils import calculate_number_of_set_conformance

            try:
                iter(elements)
            except TypeError:
                elements = [elements]

            fraction = calculate_number_of_set_conformance(elements, allowed_values) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_most_frequent_set_conformance,
                        DaQMeasures.most_frequent(column_name))

    @staticmethod
    def number_of_regex_conformance(column_name: str, regex: str | Regex) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the number of values in the window that match the
        specified regex argument. The provided ``regex`` argument has to comply with the built-in python library ``re``.
        (https://docs.python.org/3/library/re.html).
        :param column_name: the column name of pw.this table to apply the reducer on
        :param regex: the regex to check the values for matching. Has to comply with the built-in python library ``re``.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from streamdaq.utils import calculate_number_of_regex_conformance

        return pw.apply(calculate_number_of_regex_conformance, pw.reducers.tuple(pw.this[column_name]), regex)

    @staticmethod
    def fraction_of_regex_conformance(column_name: str, regex: str,
                                      precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the fraction of values in the window that match the
        specified regex argument. The provided ``regex`` argument has to comply with the built-in python library ``re``.
        (https://docs.python.org/3/library/re.html). The fraction is a float number in range [0, 1].
        :param column_name: the column name of pw.this table to apply the reducer on
        :param regex: the regex to check the values for matching. Has to comply with the built-in python library ``re``.
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_fraction_of_regex_conformance(elements: tuple):
            from streamdaq.utils import calculate_number_of_regex_conformance

            fraction = calculate_number_of_regex_conformance(elements, regex) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_regex_conformance, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def pearson(first_column_name: str, second_column_name: str,
                precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the Pearson Correlation of the values between two
        different columns on the window.
        :param first_column_name: The name of the first (x) column.
        :param second_column_name: The name of the second (y) column.
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified columns.
        """
        from streamdaq.utils import calculate_pearson_correlation

        x = pw.reducers.ndarray(pw.this[first_column_name])
        y = pw.reducers.ndarray(pw.this[second_column_name])
        return pw.apply(calculate_pearson_correlation, x, y, precision)
