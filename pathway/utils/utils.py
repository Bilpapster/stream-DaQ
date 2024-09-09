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
    Sorts timestamps and items in parallel, based on timestamps.
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
