from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List

from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.windowing import TumblingWindower, EventClock

from bytewax.operators import windowing as wop
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


@dataclass
class AppOpen:
    user: int
    time: datetime


@dataclass
class Search:
    user: int
    query: str
    time: datetime


@dataclass
class Results:
    user: int
    items: List[str]
    time: datetime


@dataclass
class ClickResult:
    user: int
    item: str
    time: datetime


# The time at which we want all of our windows to align to
align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
# Simulated events to emit into our Dataflow
# In these events we have two users, each of which
# performs a search, gets results, and clicks on a result
# User 1 searches for dogs, clicks on rover
# User 2 searches for cats, clicks on fluffy and kathy
client_events = [
    Search(user=1, time=align_to + timedelta(seconds=5), query="dogs"),
    Results(user=1, time=align_to + timedelta(seconds=6), items=["fido", "rover", "buddy"]),
    ClickResult(user=1, time=align_to + timedelta(seconds=7), item="rover"),
    Search(user=2, time=align_to + timedelta(seconds=5), query="cats"),
    Results(
     user=2,
     time=align_to + timedelta(seconds=6),
     items=["fluffy", "burrito", "kathy"],
    ),
    ClickResult(user=2, time=align_to + timedelta(seconds=7), item="fluffy"),
    ClickResult(user=2, time=align_to + timedelta(seconds=8), item="kathy"),
]


def user_event(event):
    return str(event.user), event


def calc_ctr(user__search_session):
    user, (window_metadata, search_session) = user__search_session
    print(f"First element of the tuple {user__search_session[0]}")
    print(f"Second element of the tuple {user__search_session[1]}")

    searches = [event for event in search_session if isinstance(event, Search)]
    clicks = [event for event in search_session if isinstance(event, ClickResult)]

    # See counts of searches and clicks
    print(f"User {user}: {len(searches)} searches, {len(clicks)} clicks")

    if len(searches) == 0:
        return (user, 0)
    return (user, len(clicks) / len(searches))


# Create the Dataflow
flow = Dataflow("search_ctr")
# Add input source
inp = op.input("inp", flow, TestingSource(client_events))
user_event_map = op.map("user_event", inp, user_event)
# user_event_map = op.key_on("user_event_key_on", inp, user_event)

# Collect windowed data
# Configuration for the Dataflow
event_time_config = EventClock(
    ts_getter=lambda event: event.time,
    wait_for_system_duration=timedelta(seconds=1)
)
# Configuration for the windowing operator
# clock_config = SessionWindower(gap=timedelta(seconds=10))
clock_config = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

window = wop.collect_window("windowed_data", user_event_map, clock=event_time_config, windower=clock_config)
calc = op.map("calc_ctr", window.down, calc_ctr)
op.output("out", calc, StdOutSink())
