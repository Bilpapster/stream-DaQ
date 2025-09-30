import logging
import typing
from typing import Callable, Optional, get_origin, get_args
import numpy as np

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


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


def calculate_range_conformance_count(elements: tuple, low: float, high: float, inclusive: bool | tuple[bool]) -> int:
    import numpy as np

    low, high = np.float64(low), np.float64(high)
    inclusive_low, inclusive_high = (inclusive, inclusive) if isinstance(inclusive, bool) else inclusive

    low_condition = (elements >= low) if inclusive_low else (elements > low)
    high_condition = (elements <= high) if inclusive_high else (elements < high)
    return (low_condition & high_condition).sum()


def calculate_set_conformance_count(elements: tuple, allowed_values: set):
    try:
        # first, check if elements is iterable
        iter(elements)
    except TypeError:
        # if not iterable, convert to a list
        elements = [elements]

    return sum(element in allowed_values for element in elements)


def calculate_regex_conformance_count(elements: tuple[str], regex: str):
    """
    Computes the number of elements in the tuple that match the given regex, at least once. Uses internally the Python's
    built-in library ``re``.
    :param elements: The elements to check.
    :param regex: The regex to check.
    :return: The number of elements that match the regex.
    """
    import re

    return sum(re.match(regex, element) is not None for element in elements)


def get_most_frequent_element(elements: tuple):
    frequency_dict = create_frequency_dict(elements)
    items = tuple(frequency_dict.items())
    current_max = list(items[0])
    items = items[1:]  # to avoid iterating again over the first element
    for item in items:
        if item[1] > current_max[1]:
            current_max = list(item)
            continue
        if item[1] == current_max[1]:
            if not isinstance(current_max[0], list):
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
    return [int(str(abs(number)).split(".")[0]) for number in numbers]


def extract_fractional_part(numbers: list[float]) -> list[int]:
    """
    Extracts the fractional part of every number in the provided list and returns the result as a list of integer parts.
    :param numbers: the numbers to extract the fractional part from.
    :return: a list of fractional parts (integers) where each element is the fractional part of the respective element
    of the initial list.
    """
    # Converts each number to a string, splits in two substrings using '.' and returns the second (fractional) part.
    # example: 1234.567 -> '1234.567' -> ['1234', '567'] -> '567' -> 567
    return [int(str(abs(number)).split(".")[1]) for number in numbers]


def map_to_digit_count(numbers: list[int]) -> list[int]:
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


def map_to_digit_count_in_integer_part(numbers: list[float]) -> list[int]:
    """
    Computes the number of digits in the integer parts of list elements. The result is a list of lengths.
    :param numbers: the floating point numbers to operate on.
    :return: a list of integers, corresponding to the number of digits of each given number in the initial list.
    """
    integer_parts = extract_integer_part(numbers)
    return map_to_digit_count(integer_parts)


def map_to_digit_count_in_fractional_part(numbers: list[float]) -> list[int]:
    """
    Computes the number of digits in the fractional parts of the list elements. The result is a list of lengths.
    :param numbers: the floating point numbers to operate on.
    :return: a list of integers, corresponding to the number of digits of each given number in the initial list.
    """
    fractional_parts = extract_fractional_part(numbers)
    return map_to_digit_count(fractional_parts)


def get_first_digit_frequencies(numbers: list[int], precision: int) -> dict:
    """
    Calculates the frequency of the first digits of the provided list of numbers. Uses ``extract_first_digit``
    internally. Returns a dictionary in the form {digit: [``abs_freq``, ``rel_freq``]} for all digits from 0-9.
    ``abs_freq`` stands for the absolute frequency, i.e., the actual number of appearances of that specific digit in
    the list. ``rel_freq`` stands for the relative frequency, i.e. the fraction of appearances divided by the length
    of the list. ``abs_freq`` is in range [0, ``len(numbers)``], while ``rel_freq`` is in range [0, 1].
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


def elements_satisfy_ordering(sorted_elements_by_time: tuple, ordering="ASC") -> bool:
    """
    Checks whether the elements in the provided tuple conform to the specified ordering. The result is True/False.
    :param sorted_elements_by_time: the elements to check. Note that no sorting is performed and ordering is checked
    on the tuple as is. That means you may wish to first sort the elements in chronological order, before passing them
    as argument to this function.
    :param ordering: the ordering of the elements to check for. Available options are "ASC" (strictly ascending),
    "DESC" (strictly descending), "ASC_EQ" (ascending or equal), "DESC_EQ" (descending or equal). Defaults to "ASC".
    :return:
    """

    def get_compare_function(type_literal: str) -> Callable[[int | float | str, int | float | str], bool]:
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
                return lambda first, second: first < second
            case "DESC":
                return lambda first, second: first > second
            case "ASC_EQ":
                return lambda first, second: first <= second
            case "DESC_EQ":
                return lambda first, second: first >= second
            case _:
                # if a wrong ordering is passed, do the same as "ASC"
                return lambda first, second: first < second

    compare_function = get_compare_function(ordering)
    previous = sorted_elements_by_time[0]
    sorted_elements_by_time = sorted_elements_by_time[1:]
    for element in sorted_elements_by_time:
        if not compare_function(previous, element):
            return False
        previous = element
    return True


def calculate_correlation(x, y, precision: int, method: str) -> float:
    """
    Computes the correlation/association between x and y, rounded to the specified precision.

    :param x: the x array_like values
    :param y: the y array_like values
    :param precision: the number of decimal places to include in the result
    :return: the selec correlation coefficient
    """
    from scipy.stats import kendalltau, pearsonr, spearmanr
    from scipy.stats.contingency import association

    try:
        if method == "pearson":
            result = pearsonr(x, y).statistic
        elif method == "spearman":
            result = spearmanr(x, y).statistic
        elif method == "kendall":
            result = kendalltau(x, y).statistic
        elif method == "cramer":
            observations = np.array(list(zip(x, y)))
            result = association(observations, method="cramer")
        return round(result, precision)
    except ValueError:
        # If the input arrays are empty or have different lengths, scipy will raise a ValueError
        return float("nan")


def plot_threshold_segments(
        timestamps, values, max_threshold=None, min_threshold=None, normal_color="blue", violation_color="red"
):
    """
    Plot line segments with color-coded thresholds.

    Parameters:
    - timestamps: pandas Series of timestamps
    - values: pandas Series of corresponding values
    - max_threshold: Optional upper threshold
    - min_threshold: Optional lower threshold
    - normal_color: Color for segments within thresholds
    - violation_color: Color for segments outside thresholds
    """
    import matplotlib.pyplot as plt

    # If no thresholds are set, plot everything in normal color
    if max_threshold is None and min_threshold is None:
        plt.plot(timestamps, values, color=normal_color)
        return

    # Plot segments with threshold-based coloring
    for i in range(len(timestamps) - 1):
        # Determine color based on thresholds
        current_value = values.iloc[i]
        next_value = values.iloc[i + 1]

        # Check for violation conditions
        is_violation = False
        if max_threshold is not None:
            is_violation |= (current_value > max_threshold) or (next_value > max_threshold)
        if min_threshold is not None:
            is_violation |= (current_value < min_threshold) or (next_value < min_threshold)

        # Plot segment with appropriate color
        plt.plot(
            timestamps.iloc[i: i + 2],
            values.iloc[i: i + 2],
            color=violation_color if is_violation else normal_color,
            linestyle="-",
        )

    # Add threshold lines if they exist
    if max_threshold is not None:
        plt.axhline(
            y=max_threshold,
            color=violation_color,
            linestyle="-.",
            # label='Max Threshold',
            alpha=0.3,
        )
    if min_threshold is not None:
        plt.axhline(
            y=min_threshold,
            color=violation_color,
            linestyle="-.",
            # label='Min Threshold',
            alpha=0.3,
        )


def parse_comparison_operator(expr: str) -> Optional[tuple[str, float]]:
    """
    Parse a string containing a comparison operator and a number.
    Returns tuple of (operator, number) if valid, None otherwise.

    Args:
        expr (str): Expression like ">=10" or "<5.5"

    Returns:
        Optional[tuple[str, float]]: Tuple of (operator, number) or None if invalid
    """
    # Define valid operators
    valid_operators = [">=", "<=", "==", ">", "<"]

    # Try to match the pattern: operator followed by number
    for op in valid_operators:
        if op in expr:
            try:
                number_str = expr.replace(op, "").strip()
                number = float(number_str)
                return op, number
            except ValueError:
                return None

    return None


def parse_range_expression(expr: str) -> Optional[tuple[str, float, float]]:
    """
    Parse a range expression and return the brackets and numbers.

    Args:
        expr (str): Expression like "[1,5]" or "(2.5,10)"

    Returns:
        Optional[tuple[str, float, float]]: Tuple of (brackets, lower_bound, upper_bound) or None if invalid
    """
    import re

    # Regular expression to match range patterns
    range_pattern = r"^[\(\[]([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)[\)\]]$"

    match = re.match(range_pattern, expr.strip())
    if not match:
        return None

    try:
        # Extract brackets
        brackets = expr[0] + expr[-1]
        # Extract numbers
        lower_bound = float(match.group(1))
        upper_bound = float(match.group(2))

        if lower_bound >= upper_bound:
            logger.warning("Invalid range: lower bound must be less than upper bound")
            return None

        return brackets, lower_bound, upper_bound
    except ValueError:
        return None


def create_comparison_lambda(operator: str, threshold: float) -> Callable[[float], bool]:
    """
    Create a lambda function for simple comparison operations.

    Args:
        operator (str): Comparison operator
        threshold (float): Number to compare against

    Returns:
        Callable[[float], bool]: Lambda function implementing the comparison
    """
    operator_map = {
        ">=": lambda x: x >= threshold,
        "<=": lambda x: x <= threshold,
        "==": lambda x: x == threshold,
        ">": lambda x: x > threshold,
        "<": lambda x: x < threshold,
    }

    return operator_map.get(operator, lambda x: True)


def create_range_lambda(brackets: str, lower: float, upper: float) -> Callable[[float], bool]:
    """
    Create a lambda function for range comparisons.

    Args:
        brackets (str): String containing the bracket types (e.g., '[]', '()')
        lower (float): Lower bound
        upper (float): Upper bound

    Returns:
        Callable[[float], bool]: Lambda function implementing the range check
    """
    left_bracket, right_bracket = brackets[0], brackets[1]

    def range_check(x: float) -> bool:
        left_compare = x >= lower if left_bracket == "[" else x > lower
        right_compare = x <= upper if right_bracket == "]" else x < upper
        return left_compare and right_compare

    return range_check


def create_comparison_function(expr: str) -> Callable[[float], bool]:
    """
    Main function that creates a comparison function based on the input expression.

    Args:
        expr (str): Expression string (e.g., ">=10", "[1,5]")

    Returns:
        Callable[[float], bool]: Lambda function implementing the comparison
    """
    # Handle empty or invalid input
    if not expr or not isinstance(expr, str):
        logger.warning("Invalid input. Using identity function.")
        return lambda x: True

    # Remove whitespace
    expr = expr.strip()

    # Try parsing as simple comparison
    comparison_result = parse_comparison_operator(expr)
    if comparison_result:
        operator, number = comparison_result
        return create_comparison_lambda(operator, number)

    # Try parsing as range expression
    range_result = parse_range_expression(expr)
    if range_result:
        brackets, lower, upper = range_result
        return create_range_lambda(brackets, lower, upper)

    # If nothing matches, log warning and return identity function
    logger.warning(f"Unable to parse expression: {expr}. Using identity function.")
    return lambda x: True


def keep_numbers_only(elements: tuple) -> list[float]:
    """Keeps only the numerical elements from `elements`
    and returns a new list with them.

    Args:
        elements (tuple): the tuple to keep numbers from.

    Returns:
        list[float]: a list of float numbers found in `elements`.
    """
    if not elements:
        return []

    numbers = []
    for element in elements:
        try:
            numbers.append(float(element))
        except (ValueError, TypeError):
            continue
    return numbers


def calculate_slope_best_line_fit(elements: tuple, timestamps: tuple, precision: int = 3) -> float:
    if not elements or len(elements) < 2:
        return 0.0

    numbers = []
    timestamps_of_numbers = []
    for element, timestamp in zip(elements, timestamps):
        try:
            numbers.append(float(element))
            timestamps_of_numbers.append(timestamp)
        except (ValueError, TypeError):
            continue

    if len(numbers) < 2:
        return 0.0

    # pathway cannot guarantee that tuple will be in chronological order, even all elements arrived in correct order
    # so, we need to ensure chronological ordering before computing the slope of the best line fit
    # timestamps_of_numbers, numbers = sort_by_timestamp(timestamps_of_numbers, numbers, "")
    # TODO: Debug the time-based behavior of pathway in another PR.

    import numpy as np

    # make the first timestamp be t_0=0 and then respect the original difference between them
    x = np.array(timestamps_of_numbers, dtype=float) - min(timestamps_of_numbers)
    y = np.array(numbers, dtype=float)

    try:
        # using the formula trend = \frac{Σ(x-x_μ)(y-y_μ)}{Σ(x-x_μ)^2}
        # For more information see formula of \beta here: https://en.wikipedia.org/wiki/Simple_linear_regression
        x_mean = np.mean(x)
        y_mean = np.mean(y)
        numerator = np.sum(x - x_mean * y - y_mean)
        denominator = np.sum((x - x_mean) ** 2)
        trend = numerator / denominator
        return round(trend, precision)
    except (ValueError, TypeError):
        return float("nan")

      
def construct_error_message(record: dict, error_msg: str, stream_flag=False) -> str:
    """
    Construct a formatted error message for a given event, optionally adapting
    the format for streaming contexts.

    This function combines the record's field values with the validation error message,
    while removing any redundant "For further information" lines of Pydantic. It can produce
    either a multi-line message for console output or a single-line streaming-friendly message
    for a separate deflecting stream.

    Args:
        record (dict): The data record.
                       Keys are field names and values are their corresponding values.
        error_msg (str): The raw error message returned from Pydantic validation.
        stream_flag (bool, optional): If True, returns a single-line message with errors
                                      joined by " -> ". If False (default), returns a
                                      multi-line message with record info at the top.

    Returns:
        str: The formatted error message. Returns None if `error_msg` is None.
    """
    if error_msg is None:
        return None

    # Split into lines
    lines = error_msg.splitlines()

    # Drop any "For further information..." lines
    cleaned_lines = [
        line.rstrip()
        for line in lines
        if not line.strip().startswith("For further information")
    ]

    # Put the record info at the top
    if not stream_flag:
        record_str = " | ".join(f"{k}: {v}" for k, v in record.items())
        return "\n" + record_str + "\n" + "\n".join(cleaned_lines)
    else:
        return " -> ".join(cleaned_lines)


def unpack_schema(t):
    """
    Unpack and normalize a type annotation into a string representation.

    Resolves `typing.Union` annotations by recursively
    unpacking the first type argument. If the input is a concrete
    Python type, its class name is returned. Otherwise, the object is
    converted to a string.
    Args:
        t: A type annotation or Python type.
    Returns:
        str: A string representation of the unpacked type.
    """
    if get_origin(t) is typing.Union:
        return unpack_schema(get_args(t)[0])
    elif isinstance(t, type):
        return t.__name__
    return str(t)


def extract_violation_count(error_str: str) -> int:
    """
    Extracts the first digit(s) at the start of a validation error string
    and returns them as an integer.

    - If the string is empty -> returns 0
    - If no leading digit -> returns 0
    - Otherwise -> returns the integer value

    Examples:
    "3 validation errors ..."  -> 3
    "12 validation errors ..." -> 12
    ""                         -> 0
    "validation errors ..."    -> 0
    """
    if not error_str:
        return 0

    num_str = ""
    for ch in error_str:
        if ch.isdigit():
            num_str += ch
        else:
            break

    return int(num_str) if num_str else 0

