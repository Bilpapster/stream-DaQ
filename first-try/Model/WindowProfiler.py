import sys
import faust


class WindowProfiler(faust.Record):
    max: float
    min: float
    sum: float
    sum_of_squares: float
    count: int
    mean: int
    distinct_count: int


def initialize_statistics_dictionary() -> dict:
    return {
        'max': sys.float_info.min,
        'min': sys.float_info.max,
        'sum': 0,
        'sum_squares': 0,
        'count': 0,
        'distinct': 0
    }
