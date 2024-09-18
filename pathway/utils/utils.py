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
    try:
        iter(elements)
    except TypeError:
        elements = [elements]

    return sum(element in allowed_values for element in elements)


def calculate_number_of_regex_conformance(elements: tuple[str], regex: str):
    """
    Computes the number of elements in the tuple that match the given regex, at least once. Uses internally the Python's
    built-in library ``re``.
    :param elements: The elements to check.
    :param regex: The regex to check.
    :return: The number of elements that match the regex.
    """
    import re

    return sum(re.match(regex, element) is not None for element in elements)


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


def get_percentiles(elements: list, percentiles: int | list, precision: int) -> dict:
    """
    Computes the specified percentile(s) on the given elements. ``percentiles`` argument can either be a single integer
    value or a list of integers. The result is a dictionary in the form {percentile: value} for all ``percentiles``
    :param elements: the elements to compute the percentiles on
    :param percentiles: the percentiles to compute. Can be either a single integer value or a list of integers.
    :param precision: the number of decimal points to include in the percentile values
    :return: a percentiles dictionary in the form {percentile: value} for all ``percentiles``.
    """
    import numpy as np
    result = np.percentile(elements, percentiles, overwrite_input=True)
    result = np.round(result, precision)
    return {percentile: value for percentile, value in zip(percentiles, result)}


def extract_first_digit(numbers: list[int]) -> list[int]:
    """
    Extracts the first digit of every number in the provided list and returns the result as a list of digits (integers).
    :param numbers: the numbers to extract the first digit from.
    :return: a list of digits (integers) where each element is the first digit of the respective element of the initial
    list.
    """
    # extracting the first digit via transforming the number to string and then back to number
    # seems to be the most performant method, according to
    # https://stackoverflow.com/questions/41271299/how-can-i-get-the-first-two-digits-of-a-number
    return [int(str(abs(number))[0]) for number in numbers]


def extract_integer_part(numbers: list[float]) -> list[int]:
    """
    Extracts the integer part of every number in the provided list and returns the result as a list of integer parts.
    :param numbers: the numbers to extract the integer part from.
    :return: a list of integer parts (integers) where each element is the integer part of the respective element of
    the initial list.
    """
    # Converts each number to a string, splits in two substrings using '.' and returns the first (integer) part.
    # example: 1234.567 -> '1234.567' -> ['1234', '567'] -> '1234' -> 1234
    return [int(str(abs(number)).split('.')[0]) for number in numbers]


def extract_fractional_part(numbers: list[float]) -> list[int]:
    """
    Extracts the fractional part of every number in the provided list and returns the result as a list of integer parts.
    :param numbers: the numbers to extract the fractional part from.
    :return: a list of fractional parts (integers) where each element is the fractional part of the respective element
    of the initial list.
    """
    # Converts each number to a string, splits in two substrings using '.' and returns the second (fractional) part.
    # example: 1234.567 -> '1234.567' -> ['1234', '567'] -> '567' -> 567
    return [int(str(abs(number)).split('.')[1]) for number in numbers]


def map_to_number_of_digits(numbers: list[int]) -> list[int]:
    """
    Transforms a list of integer numbers into a list of (their) lengths, i.e. number of digits. In other words, the
    function maps each number in the given list to its length. For example, the list [1, 23, 456, 7890] is
    transformed to [1, 2, 3, 4].
    :param numbers: the numbers to transform.
    :return: a list of integers, corresponding to the number of digits of each number in the initial list.
    """
    return [len(str(number)) for number in numbers]

def map_to_length(elements: list[str]) -> list[int]:
    """
    Transforms a list of string elements into a list of (their) lengths. In other words, the
    function maps each element in the given list to its length. For example, the list ['S', 'tr', 'eam', 'DaQ!'] is
    transformed to [1, 2, 3, 4].
    :param elements: the elements to transform.
    :return: a list of integers, corresponding to the length of each string element in the initial list.
    """
    return [len(element) for element in elements]


def compute_number_of_digits_in_integer_parts(numbers: list[float]) -> list[int]:
    """
    Computes and returns the number of digits in the integer parts of the provided list. The result is a list of lengths.
    :param numbers: the floating point numbers to operate on.
    :return: a list of integers, corresponding to the number of digits of each given number in the initial list.
    """
    integer_parts = extract_integer_part(numbers)
    return map_to_number_of_digits(integer_parts)


def compute_number_of_digits_in_fractional_parts(numbers: list[float]) -> list[int]:
    """
    Computes and returns the number of digits in the fractional parts of the provided list. The result is a list of lengths.
    :param numbers: the floating point numbers to operate on.
    :return: a list of integers, corresponding to the number of digits of each given number in the initial list.
    """
    fractional_parts = extract_fractional_part(numbers)
    return map_to_number_of_digits(fractional_parts)


def get_first_digit_frequencies(numbers: list[int], precision: int) -> dict:
    """
    Calculates and returns the frequency of the first digits of the provided list of numbers. Uses ``extract_first_digit``
    internally. Returns a dictionary in the form {digit: [``abs_freq``, ``rel_freq``]} for all digits from 0-9. ``abs_freq`` stands
    for the absolute frequency, i.e., the actual number of appearances of that specific digit in the list. ``rel_freq``
    stands for the relative frequency, i.e. the fraction of appearances divided by the length of the list. ``abs_freq`` is
    in range [0, ``len(numbers)``], while ``rel_freq`` is in range [0, 1].
    :param numbers: the numbers to calculate the first digits' frequencies from.
    :param precision: the number of decimal points to include in the relative frequency values.
    :return:
    """
    first_digits = extract_first_digit(numbers)
    frequencies = [0] * 10  # [0 appearances] * 10_different_digits, creates a list with 10 zeros [0, 0, ..., 0]
    for digit in first_digits:
        frequencies[digit] += 1

    length = len(first_digits)
    return {digit: [frequency, round(frequency / length, precision)] for digit, frequency in enumerate(frequencies)}


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


def calculate_pearson_correlation(x, y, precision: int) -> float:
    """
    Computes the Pearson correlation between x and y, rounded to the specified precision. Leverages internally
    the relative implementation from scipy:
    https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html
    :param x: the x array_like values
    :param y: the y array_like values
    :param precision: the number of decimal places to include in the result
    :return: the Pearson correlation coefficient
    """
    from scipy.stats import pearsonr
    result = pearsonr(x, y)
    return round(result.statistic, precision)
    # return round(result.statistic, precision), round(result.pvalue, precision)
