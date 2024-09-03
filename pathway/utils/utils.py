def calculate_number_of_unique_values(elements: list) -> int:
    import numpy as np

    frequency_dict = dict()
    for element in elements:
        key = str(element)
        frequency_dict[key] = frequency_dict.get(key, 0) + 1
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
