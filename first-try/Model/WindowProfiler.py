import sys
import faust


class WindowProfiler(faust.Record):
    timestamp: float
    max: float
    min: float
    mean: float
    count: int
    distinct: int


def initialize_statistics_dictionary() -> dict:
    return {
        'max': sys.float_info.min,
        'min': sys.float_info.max,
        'sum': 0,
        'sum_squares': 0,
        'count': 0,
        'distinct': set()
    }
