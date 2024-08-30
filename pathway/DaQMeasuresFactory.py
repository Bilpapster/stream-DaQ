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
    def get_avg_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        Static getter to retrieve an avg pathway reducer, applied on current table (pw.this) and in the column specified
        by column name
        :param column_name: the column name of pw.this table to apply the avg reducer on.
        :return: a pathway avg reducer
        """
        return pw.reducers.avg(pw.this[column_name])

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
        todo: It is better if we use this in an incremental way, by extending pw.BaseCustomAccumulator
        todo: update this function, so that it returns a custom reducer https://pathway.com/developers/user-guide/data-transformation/custom-reducers
        Static getter to retrieve a custom reducer that computes the approximate number of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 0 (round to integer).
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_approx_number_of_distinct_values(elements: list) -> float:
            from datasketch import HyperLogLogPlusPlus
            # https://ekzhu.com/datasketch/

            hpp = HyperLogLogPlusPlus()
            for element in elements:
                hpp.update(str(element).encode('utf-8'))
            approx_distinct = hpp.count()
            return round(approx_distinct, precision)

        return pw.apply(get_approx_number_of_distinct_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def get_approx_fraction_of_distinct_values_reducer(column_name: str,
                                                       precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        todo: It is better if we use this in an incremental way, by extending pw.BaseCustomAccumulator
        todo: update this function, so that it returns a custom reducer https://pathway.com/developers/user-guide/data-transformation/custom-reducers
        Static getter to retrieve a custom reducer that computes the approximate fraction of distinct elements in the
        window, using the HyperLogLog++ sketch. The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_approx_fraction_of_distinct_values(elements: list) -> float:
            from datasketch import HyperLogLogPlusPlus
            # https://ekzhu.com/datasketch/

            hpp = HyperLogLogPlusPlus()
            for element in elements:
                hpp.update(str(element).encode('utf-8'))
            approx_distinct_fraction = hpp.count() / len(elements)
            return round(approx_distinct_fraction, precision)

        return pw.apply(get_approx_fraction_of_distinct_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def get_number_of_unique_values_reducer(column_name: str) -> pw.internals.expression.ColumnExpression:
        """
        todo: Explicitly state the difference between unique (only one appearance) and distinct (at least one appearance)
        Static getter to retrieve a custom reducer that computes the number of unique elements in the window.
        The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :return: a pathway pw.apply statement ready for use as a column
        """
        def get_number_of_unique_values(elements: list):
            import numpy as np

            frequency_dict = dict()
            for element in elements:
                key = str(element)
                frequency_dict[key] = frequency_dict.get(key, 0) + 1
            frequencies = frequency_dict.values()
            return np.sum(frequency == 1 for frequency in frequencies)

        return pw.apply(get_number_of_unique_values, pw.reducers.tuple(pw.this[column_name]))

    @staticmethod
    def get_fraction_of_unique_values_reducer(column_name: str,
                                              precision: int = 3) -> pw.internals.expression.ColumnExpression:
        """
        todo: Explicitly state the difference between unique (only one appearance) and distinct (at least one appearance)
        Static getter to retrieve a custom reducer that computes the fraction of unique elements in the window.
        The fraction is in range [0, 1]
        :param column_name: the column name of pw.this table to apply the reducer on
        :param precision: the number of decimal points to include in the fraction result. Defaults to 3.
        :return: a pathway pw.apply statement ready for use as a column
        """

        def get_fraction_of_unique_values(elements: list):
            import numpy as np

            frequency_dict = dict()
            for element in elements:
                key = str(element)
                frequency_dict[key] = frequency_dict.get(key, 0) + 1
            frequencies = frequency_dict.values()
            fraction = np.sum(frequency == 1 for frequency in frequencies) / len(elements)
            return round(fraction, precision)

        return pw.apply(get_fraction_of_unique_values, pw.reducers.tuple(pw.this[column_name]))

# todo: extract re-used functions to an external utility class to develop it in one place an use it wherever needed
# e.g., get_number_above_mean, get_number_of_distinct, etc and reuse for the fraction, dividing by the list length