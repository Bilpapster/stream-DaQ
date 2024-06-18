# example taken from the bytewax's official documentation guide
# https://docs.bytewax.io/stable/guide/getting-started/collecting-windowing-example.html

from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import TestingSource

flow = Dataflow("collect_example")
stream = op.input("input", flow, TestingSource(list(range(10))))
# we want to consider all the items together, so we assign the same fixed key to each of them
keyed_stream = op.key_on("key", stream, lambda _x: "ALL")
collected_stream = op.collect(
    "collect", keyed_stream, timeout=timedelta(seconds=10), max_size=3
)
op.output("out", collected_stream, StdOutSink())
