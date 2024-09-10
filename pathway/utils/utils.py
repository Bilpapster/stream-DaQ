def create_frequency_dict(elements: tuple) -> dict:
    frequency_dict = dict()
    for element in elements:
        key = element
        frequency_dict[key] = frequency_dict.get(key, 0) + 1
    return frequency_dict


def calculate_number_of_unique_values(elements: tuple) -> int:
    import numpy as np

    frequency_dict = create_frequency_dict(elements)
    frequencies = frequency_dict.values()
    number_of_unique_elements = np.sum(frequency == 1 for frequency in frequencies)
    return number_of_unique_elements


def calculate_fraction(numerator: float, denominator: float, precision: int) -> float:
    return round(numerator / denominator, precision)


def calculate_median(elements: tuple):
    from statistics import median
    return median(elements)


def calculate_number_of_range_conformance(elements: tuple, low: float, high: float, inclusive: bool) -> int:
    import numpy as np

    low, high = np.float64(low), np.float64(high)
    low_condition = (elements >= low) if inclusive else (elements > low)
    high_condition = (elements <= high) if inclusive else (elements < high)
    return (low_condition & high_condition).sum()


def calculate_number_of_set_conformance(elements: tuple, allowed_values: set):
    return sum(element in allowed_values for element in elements)


def find_most_frequent_element(elements: tuple):
    frequency_dict = create_frequency_dict(elements)
    items = tuple(frequency_dict.items())
    current_max = list(items[0])
    items = items[1:]  # to avoid iterating again over the first element
    for item in items:
        if item[1] > current_max[1]:
            current_max = list(item)
            continue
        if item[1] == current_max[1]:
            if type(current_max[0]) != list:
                current_max[0] = [current_max[0]]
            current_max[0].append(item[0])
    return current_max  # a tuple in the form (most_frequent_element, current_max_frequency)


def sort_by_timestamp(timestamps: list, elements: list[int | float | str], time_format: str) -> tuple:
    """
    Sorts timestamps and items in parallel, based on timestamps (chronological order).
    :param timestamps: a list of timestamps as floats
    :param elements: a list of items as floats, corresponding to the timestamps
    :param time_format: the time format in which the string values of the timestamp column are provided
    :return: the list of items sorted, based on their timestamps
    """
    import numpy as np

    # IMPORTANT: timestamps are objects of the pandas.Timestamp class (Pathway reads datetime in that way if you use the
    # .dt utility, as presented in their tutorials, e.g.,
    # https://pathway.com/developers/user-guide/temporal-data/windows-manual#windowby-reduce)
    elements, timestamps = np.array(elements), np.array(timestamps)
    timestamps_int = np.array([timestamp.value // 1e9 for timestamp in timestamps])
    sorted_indices = np.argsort(timestamps_int)
    timestamps_sorted, elements_sorted_by_timestamp = timestamps[sorted_indices], elements[sorted_indices]
    return timestamps_sorted, elements_sorted_by_timestamp


def check_ordering(sorted_elements_by_time: tuple, ordering="ASC"):
    """
    Checks whether the elements in the provided tuple conform to the specified ordering. The result is True/False.
    :param sorted_elements_by_time: the elements to check. Note that no sorting is performed and ordering is checked
    on the tuple as is. That means you may wish to first sort the elements in chronological order, before passing them
    as argument to this function.
    :param ordering: the ordering of the elements to check for. Available options are "ASC" (strictly ascending),
    "DESC" (strictly descending), "ASC_EQ" (ascending or equal), "DESC_EQ" (descending or equal). Defaults to "ASC".
    :return:
    """

    def get_compare_function(type_literal: str):
        """
        A higher-order auxiliary function that returns a comparison function, based on the type_literal argument.
        Available options for the type literal are the following: \n
        - "ASC": first element strictly smaller than the second \n
        - "ASC_EQ": first element smaller or equal to the second \n
        - "DESC": first element strictly greater than the second \n
        - "DESC_EQ": first element greater or equal to the second \n
        :param type_literal: a string to specify the comparison function to be returned. Accepted options are "ASC",
        "ASC_EQ", "DESC", "DESC_EQ". In case the provided string is not one of the accepted options, "ASC" comparison
        function is returned by default. For details about the comparison function returned in each of the options, see
        above.
        :return: a comparison function between two elements
        """
        # match-case enabled in python, starting from version 3.10
        match type_literal:
            case "ASC":
                function = lambda first, second: first < second
            case "DESC":
                function = lambda first, second: first > second
            case "ASC_EQ":
                function = lambda first, second: first <= second
            case "DESC_EQ":
                function = lambda first, second: first >= second
            case _:
                function = lambda first, second: first < second  # if a wrong ordering is passed, do the same as "ASC"

        return function

    compare_function = get_compare_function(ordering)
    previous = sorted_elements_by_time[0]
    sorted_elements_by_time = sorted_elements_by_time[1:]
    for element in sorted_elements_by_time:
        if not compare_function(previous, element):
            return False
        previous = element
    return True
