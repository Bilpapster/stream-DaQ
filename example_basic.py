# pip install streamdaq

from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows


def seven_is_frequent(most_frequent: int | float | tuple) -> bool:
    """
    A simple assessment function that checks whether number 7 is contained in most frequent items.
    :param most_frequent: a tuple or single value denoting the most frequent items.
    :return: true if number 7 is contained in most frequent items, false otherwise.
    """
    from collections.abc import Iterable

    # if most frequent items are more than one, check for set membership
    if isinstance(most_frequent, Iterable):
        return 7 in most_frequent

    # if there is a single most frequent item, check just for equality
    return most_frequent == 7


daq = StreamDaQ().configure(
    window=Windows.tumbling(3),
    instance="user_id",
    time_column="timestamp",
    wait_for_late=1,
    time_format='%Y-%m-%d %H:%M:%S'
)

# Step 2: Define what Data Quality means for you
daq.add(dqm.count('interaction_events'), assess="(5, 15]", name="count") \
    .add(dqm.min('interaction_events'), assess="<=6", name="min_interact") \
    .add(dqm.max('interaction_events'), assess=">5.09", name="max_interact") \
    .add(dqm.median('interaction_events'), assess="[3, 8]", name="med_interact") \
    .add(dqm.most_frequent('interaction_events'), assess=seven_is_frequent, name="freq_interact") \
    .add(dqm.number_of_distinct_approx('interaction_events'), assess="==9", name="approx_dist_interact") \
    .add(dqm.number_of_distinct('interaction_events'), assess="==9", name="dist_interact")

# Complete list of Data Quality Measures (dqm): https://github.com/Bilpapster/stream-DaQ/blob/main/streamdaq/DaQMeasures.py
# todo add complete list of available assessment function options with both string literals and callback functions

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
