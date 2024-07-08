from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List

from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSink
from bytewax.operators.windowing import TumblingWindower, EventClock

from bytewax.operators import windowing as wop
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

from pathlib import Path

from data_generator import create_experiment_input


def on_window_close(user__search_session):
    import time

    start = time.time()
    key, (window_id, window_contents) = user__search_session
    durations = [event["duration_watched"] for event in window_contents]
    return (str(window_id), (
        max(durations),
        min(durations),
        sum(durations) / len(durations),
        len(durations),
        len(set([round(duration) for duration in durations])),
        float(time.time() - start)
    ))


def stringify(aggregate_result):
    # aggregate results are in the form (key, (max, min, mean))
    (window_id, ((max, min, mean, count, distinct, processing_time), window_metadata)) = aggregate_result
    timestamp = window_metadata.open_time
    return str(window_id), f"{timestamp},{window_id},{max},{min},{mean},{count},{distinct},{processing_time}"


def key_on_window_metadata_id(metadata_object):
    key, (window_id, window_metadata) = metadata_object
    return (str(window_id), window_metadata)


ALIGN_TO = datetime(year=2024, month=1, day=1, tzinfo=timezone.utc)
WINDOW = timedelta(seconds=10)

flow = Dataflow("stream_DaQ")
stream = op.input("input", flow, TestingSource(create_experiment_input()))
keyed_stream = op.map("key_on_user_id", stream, lambda record: (str(record["user_id"]), record))

clock = EventClock(ts_getter=lambda record: record["timestamp"], wait_for_system_duration=timedelta(seconds=1))
windower = TumblingWindower(length=WINDOW, align_to=ALIGN_TO)
window = wop.collect_window("window", up=keyed_stream, clock=clock, windower=windower)
aggregated_statistics = op.map("calculate_statistics", window.down, on_window_close)

window_metadata = op.map("key_metadata", window.meta, key_on_window_metadata_id)
enriched_results = op.join("join", aggregated_statistics, window_metadata)
flattened_results = op.map("to_string", enriched_results, stringify)
op.output("persist_results", flattened_results, FileSink(Path("data_bytewax_1.csv")))
