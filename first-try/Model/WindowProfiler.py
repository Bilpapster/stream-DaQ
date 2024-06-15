import sys


def initialize_statistics_dictionary() -> dict:
    return {
        'max': sys.float_info.min,
        'min': sys.float_info.max,
        'sum': 0,
        'sum_squares': 0,
        'count': 0,
        'distinct': set()
    }
