import pathway as pw
from datetime import timedelta, datetime
from typing import Callable, Any

from pathway.stdlib.temporal import Window


def tumbling(duration: int | float | timedelta, origin: int | float | datetime | None = None) -> Window:
    """
    A pathway tumbling window generator with the specified duration and origin.
    :param duration: the duration of the tumbling window. It can be either an integer or float or a timedelta.
    :param origin: a point in time to start the first window from. Defaults to None (non-specified origin).
    :return: a pathway tumbling window.
    """
    return pw.temporal.tumbling(duration=duration, origin=origin)


def sliding(
    hop: int | float | timedelta,
    duration: int | float | timedelta | None = None,
    ratio: int | None = None,
    origin: int | float | datetime | None = None,
) -> Window:
    """
    A pathway sliding window generator with the specified hop, duration, ratio and origin.
    :param hop: the frequency of the window.
    :param duration: the length of the window.
    :param ratio: used as an alternative way to specify the duration as hop * ratio.
    Defaults to None (explicit duration argument)
    :param origin: a point in time to start the first window from. Defaults to None.
    :return: a pathway sliding window.
    """
    return pw.temporal.sliding(hop=hop, duration=duration, ratio=ratio, origin=origin)


def session(predicate: Callable[[Any, Any], bool] = None, max_gap: int | float | timedelta | None = None) -> Window:
    """
    A pathway session window generator with the specified predicate or max_gap. Note: exactly one of the two arguments
    must be passed to the function. In case predicate is passed, max_gap will be ignored.
    :param predicate: function taking two adjacent entries that returns a boolean saying whether the two entries
    should be grouped.
    :param max_gap: two adjacent entries will be grouped if `b - a < max_gap`.
    :return: a pathway session window.
    """
    if predicate:
        return pw.temporal.session(predicate=predicate)
    return pw.temporal.session(max_gap=max_gap)
