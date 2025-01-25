from pathlib import Path
from config.configurations import low_threshold, high_threshold


def is_out_of_range(value):
    if value is None:
        return False  # do not count missing values as "out of range"
    return not low_threshold <= value <= high_threshold


def standardize_timestamp_to_nanoseconds(floating_point_timestamp: float) -> int:
    return int(floating_point_timestamp * 1e3)


def set_up_output_path(filename: str):
    import os
    output_file = Path(filename)
    output_file.parent.mkdir(exist_ok=True, parents=True)
    try:
        os.remove(filename)
    except OSError:
        pass
