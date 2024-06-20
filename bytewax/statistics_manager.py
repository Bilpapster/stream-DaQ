from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.windowing import EventClock, TumblingWindower, WindowMetadata
from bytewax.testing import TestingSource

import sys
import pandas as pd
import random

sys.path.append('../datasets')
dataset = pd.read_csv('../datasets/streaming_viewership_data.csv')

inp = []
timestamp = datetime(year=2024, month=6, day=20, tzinfo=timezone.utc)
for row_index in range(len(dataset)):
    row = dataset.iloc[row_index]

    if random.random() < 0.1:
        timestamp += timedelta(seconds=random.randint(1, 3))

    inp.append(
        {
            "timestamp": timestamp,
            "user_id": "TestUser",
            "session_id": str(row['Session_ID']),
            "device_id": str(row['Device_ID']),
            "video_id": str(row['Video_ID']),
            "duration_watched": float(row['Duration_Watched (minutes)']),
            "genre": str(row['Genre']),
            "country": str(row['Country']),
            "age": int(row['Age']),
            "gender": str(row['Gender']),
            "subscription_status": str(row['Subscription_Status']),
            "ratings": int(row['Ratings']),
            "languages": str(row['Languages']),
            "device_type": str(row['Device_Type']),
            "location": str(row['Location']),
            "playback_quality": str(row['Playback_Quality']),
            "interaction_events": int(row['Interaction_Events'])
        }
    )

flow = Dataflow("statistical_manager")
align_to = datetime(year=2024, month=6, day=20, tzinfo=timezone.utc)
stream = op.input("input", flow, TestingSource(inp))
keyed_stream = op.key_on("key_on_user_id", stream, lambda record: record["user_id"])
# keyed_stream = op.map_value("keeping_only_duration", keyed_stream, lambda record: record["duration_watched"])
clock = EventClock(lambda record: datetime.fromtimestamp(record["timestamp"]), wait_for_system_duration=timedelta(seconds=1))
windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)
window_max = win.collect_window("window_max", keyed_stream, clock, windower)

op.inspect("check_window_max", window_max.down)

# todo check this tutorial https://github.com/bytewax/search-session
# since it seems promising about handling windows and metrics inside them


#
#
# window_min = win.min_window("window_min", keyed_stream, clock, windower)
#







