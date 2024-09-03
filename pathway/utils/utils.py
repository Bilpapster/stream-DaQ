def calculate_number_of_unique_values(elements: list):
    import numpy as np

    frequency_dict = dict()
    for element in elements:
        key = str(element)
        frequency_dict[key] = frequency_dict.get(key, 0) + 1
    frequencies = frequency_dict.values()
    number_of_unique_elements = np.sum(frequency == 1 for frequency in frequencies)
    return number_of_unique_elements


def calculate_fraction(numerator: float, denominator: float, precision: int):
    return round(numerator / denominator, precision)