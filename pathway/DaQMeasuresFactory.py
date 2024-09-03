import pathway as pw


class DaQMeasuresFactory:
    @staticmethod
    def get_min_reducer(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a min pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the min reducer on.
        :return: a pathway min reducer
        """
        return pw.reducers.min(pw.this[column_name])

    @staticmethod
    def get_max_reducer(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a max pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the max reducer on.
        :return: a pathway max reducer
        """
        return pw.reducers.max(pw.this[column_name])

    @staticmethod
    def get_count_reducer(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a count pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the count reducer on.
        :return: a pathway count reducer
        """
        return pw.reducers.count(pw.this[column_name])

    @staticmethod
    def get_avg_reducer(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve an avg pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the avg reducer on.
        :param precision: the number of decimal points to include in the result. Defaults to 3.
        :return: a pathway avg reducer
        """
        return pw.apply(round, pw.reducers.avg(pw.this[column_name]), precision)

    @staticmethod
    def get_median_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a median pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the median reducer on.
        :return: a pathway avg reducer
        """
        from utils.utils import calculate_median
        return pw.apply(calculate_median, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def get_ndarray_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve an ndarray pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the ndarray reducer on.
        :return: a pathway ndarray reducer
        """
        return pw.reducers.ndarray(pw.this[column_name])

    @staticmethod
    def get_tuple_reducer(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a tuple pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the tuple reducer on.
        :return: a pathway tuple reducer
        """
        return pw.reducers.tuple(pw.this[column_name])

    @staticmethod
    def get_sorted_tuple_reducer(column_name: str) -> pw.internals.expression.ReducerExpression:
        """
        Static getter to retrieve a sorted_tuple pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the sorted_tuple reducer on.
        :return: a pathway sorted_tuple reducer
        """
        return pw.reducers.sorted_tuple(pw.this[column_name])

    @staticmethod
    def get_number_of_values_above_mean_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
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
    def get_fraction_of_values_above_mean_reducer(column_name: str,
                                                  precision: int = 3) -> pw.internals.expression.ColumnExpression:
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
    def get_number_of_distinct_values_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
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
    def get_fraction_of_distinct_values_reducer(column_name: str,
                                                precision: int = 3) -> pw.internals.expression.ColumnExpression:
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
    def get_approx_number_of_distinct_values_reducer(column_name: str,
                                                     precision: int = 0) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the approximate number of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 0 (round to integer).
        :return: a pathway pw.apply statement ready for use as a column
        """
        from _custom_reducers.CustomReducers import approx_distinct_count_reducer

        return pw.apply(round, approx_distinct_count_reducer(pw.this[column_name]), precision)

    @staticmethod
    def get_approx_fraction_of_distinct_values_reducer(column_name: str,
                                                       precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the approximate fraction of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """
        from _custom_reducers.CustomReducers import approx_distinct_count_reducer

        def get_approx_fraction_of_distinct_values(distinct_count: float, total_count: int) -> float:
            from utils.utils import calculate_fraction

            return calculate_fraction(distinct_count, total_count, precision)

        return pw.apply(get_approx_fraction_of_distinct_values, approx_distinct_count_reducer(pw.this[column_name]),
                        pw.reducers.count(pw.this[column_name]))

    @staticmethod
    def get_number_of_unique_values_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
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
            from utils.utils import calculate_number_of_unique_values
            return calculate_number_of_unique_values(elements)

        return pw.apply(get_number_of_unique_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def get_fraction_of_unique_values_reducer(column_name: str,
                                              precision: int = 3) -> pw.internals.expression.ColumnExpression:
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

        def get_fraction_of_unique_values(elements: list):
            from utils.utils import calculate_number_of_unique_values

            fraction = calculate_number_of_unique_values(elements) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_unique_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def get_std_dev_reducer(column_name: str, precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the standard deviation of the values in the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """
        from _custom_reducers.CustomReducers import std_dev_reducer

        return pw.apply(round, std_dev_reducer(pw.this[column_name]), precision)

    @staticmethod
    def get_percentiles_reducer(column_name: str, percentiles: int | list,
                                precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve a custom reducer that computes the specified percentiles of the values in the window.
        :param column_name: the column name of pw.this table to apply the reducer on
        :param percentiles: a single (int) or multiple percentiles (list of int) to compute
        :param precision: the number of decimal points to include in the result. Defaults to 3. In case multiple
        percentiles are given, all percentile values are rounded to the same number of decimal points.
        :return: a pw.ColumnExpression that corresponds to the application of the custom reducer on the specified column
        """

        def get_percentiles(elements: list):
            import numpy as np
            result = np.percentile(elements, percentiles, overwrite_input=True)
            result = np.round(result, precision)
            return {percentile: value for percentile, value in zip(percentiles, result)}

        return pw.apply(get_percentiles, pw.reducers.ndarray(pw.this[column_name]))
