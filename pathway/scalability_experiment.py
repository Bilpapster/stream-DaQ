from DaQMeasures import DaQMeasures as dqm
from Windows import tumbling, sliding, session

import os, time
import pathway as pw
from datetime import timedelta, datetime
from typing import Self

from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

# Get configuration from environment variables
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
SLIDE_DURATION_STR = os.getenv('SLIDE_DURATION', '5 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
MESSAGES_PER_WINDOW_LIST = os.getenv('MESSAGES_PER_WINDOW', '1000')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()

print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
print(f"KAFKA_SERVER: {KAFKA_SERVER}")
print(f"WINDOW_DURATION_STR: {WINDOW_DURATION_STR}")
print(f"MESSAGES_PER_WINDOW_LIST: {MESSAGES_PER_WINDOW_LIST}")
print(f"WINDOW_TYPE: {WINDOW_TYPE}")
print(f"GAP_DURATION_STR: {GAP_DURATION_STR}")

def standardize_timestamp_to_milliseconds(_) -> str:
    return str(int(time.time() * 1e3))

def parse_duration(duration_str):
    # Parses '10 seconds' into 10.0
    units = {'s': 1, 'seconds': 1, 'sec': 1, 'secs': 1, 'second': 1,
             'm': 60, 'minutes': 60, 'min': 60, 'mins': 60, 'minute': 60}
    parts = duration_str.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid duration format: {duration_str}")
    value = float(parts[0])
    unit = parts[1].lower()
    if unit not in units:
        raise ValueError(f"Unknown unit in duration: {unit}")
    return value * units[unit]

def get_window_from_string(window_type_string: str):
    window_str = window_type_string.lower()
    match window_str:
        case 'tumbling':
            return tumbling(duration=int(parse_duration(WINDOW_DURATION_STR)*1000), origin=0) # * 1000 to make it milliseconds
        case 'sliding':
            return sliding(
                duration=int(parse_duration(WINDOW_DURATION_STR)*1000),
                hop=int(parse_duration(SLIDE_DURATION_STR) * 1000),
                origin=0
            )
        case 'session':
            return session(max_gap=parse_duration(GAP_DURATION_STR)*1000)
        case _:
            print(f"Unknown window type: {window_str}. Falling back to tumbling.")
            return tumbling(duration=parse_duration(WINDOW_DURATION_STR) * 1000)

class StreamDaQ:
    """
    The fundamental class of the DQ monitoring system. An instance of this class is necessary and sufficient to
    perform the following actions/steps: \n
    1. Configure the monitoring details, such as the type and parameters of the monitoring window, the way late events
    are handled and the source/sink details. \n
    2. Define what **exactly** DQ means for your unique use-case, by adding DQ measurements of your data stream, from
    a palette of built-in, real-time measurements. \n
    3. Kick-off DQ monitoring of your data stream, letting Stream DaQ continuously watch out your data, while you
    focus on the important.
    """

    def __init__(self):
        """
        Class constructor. Initializes to default/None values or potentially useful class arguments.
        """
        from collections import OrderedDict
        self.measures = OrderedDict()
        self.window = None
        self.instance = None
        self.time_column = None
        self.wait_for_late = None
        self.time_format = None
        self.show_window_start = True
        self.show_window_end = True
        self.sink_file_name = None

    def configure(self, window: Window, instance: str, time_column: str,
                  wait_for_late: int | float | timedelta | None = None,
                  time_format: str = "%Y-%m-%d %H:%M:%S.%f", show_window_start: bool = True,
                  show_window_end: bool = True, sink_file_name: str = None) -> Self:
        """
        Configures the DQ monitoring parameters. Specifying a window object, the key instance and the time column name
        cannot be omitted. The rest of the arguments are optional and come with rational default values.
        :param window: a window object to use for widowing the source stream.
        :param instance: the name of the column that contains the key for each incoming element.
        :param time_column: the name of the column that contains the date/time information for every element.
        :param wait_for_late: the number of seconds to wait after the end timestamp of each window. Late elements that
        arrive more than `wait_for_late` seconds after the window is closed will be ignored.
        :param time_format: the format of the values in the column that contains date/time information
        :param show_window_start: boolean flag to specify whether the window starting timestamp should be included in
        the results
        :param show_window_end: boolean flag to specify whether the window ending timestamp should be included in
        the results
        :param sink_file_name: the name of the file to write the output to
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        self.window = window
        self.instance = instance
        self.time_column = time_column
        self.wait_for_late = wait_for_late
        self.time_format = time_format
        self.show_window_start = show_window_start

        self.show_window_end = show_window_end
        if self.show_window_start:
            self.measures['window_start'] = pw.this._pw_window_start
        if self.show_window_end:
            self.measures['window_end'] = pw.this._pw_window_end

        self.sink_file_name = sink_file_name
        return self

    def add(self, measure: pw.ColumnExpression | ReducerExpression, name: str) -> Self:
        """
        Adds a DQ measurement to be monitored within the stream windows.
        :param measure: the measure to be monitored
        :param name: the name with which the measure will appear in the results
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        self.measures[name] = measure
        return self

    def watch_out(self):
        """
        Kicks-off the monitoring process. Calling this function at the end of your driver program is necessary, or else
        nothing of what you have declared before will be executed.
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        rdkafka_settings = {
            "bootstrap.servers": KAFKA_SERVER,
            "security.protocol": "plaintext",
            "group.id": "0",
            "session.timeout.ms": "6000",
            "auto.offset.reset": "earliest",
        }

        class InputSchema(pw.Schema):
            """
            Schema for the input stream elements that are read from the Kafka topic.
            To keep it simple, we assume that each element is just an int (nullable) value.
            """
            id_: str | None
            productName: str | None
            description: str | None
            priority: str | None
            numViews: int | None
            eventTime: int

        data = (pw.io.kafka.read(
            rdkafka_settings,
            topic=KAFKA_TOPIC,
            format="json",
            schema=InputSchema,
            autocommit_duration_ms=1
        ))

        data = (data.windowby(
            data.eventTime,
            window=self.window,
            behavior=pw.temporal.exactly_once_behavior(shift=self.wait_for_late),
        ).reduce(**self.measures))

        pw.io.csv.write(data, self.sink_file_name)
        pw.run()


# Step 1: Configure monitoring parameters
daq = StreamDaQ().configure(
    window=get_window_from_string(WINDOW_TYPE),
    instance="instanceColumn",
    time_column="eventTime",
    wait_for_late=0,
    time_format="%Y-%m-%d %H:%M:%S.%f",
    show_window_start=True,
    show_window_end=True,
    sink_file_name=f"data/daq_{WINDOW_TYPE}_{WINDOW_DURATION_STR}_{SLIDE_DURATION_STR}_{GAP_DURATION_STR}.csv",
)

# Step 2: Define what Data Quality means for you
daq.add(dqm.count('id_'), "count") \
    .add(dqm.fraction_of_unique("id_"), "is_id_unique") \
    .add(dqm.max('numViews'), "max_views") \
    .add(dqm.fraction_of_set_conformance("priority", {"low", "medium", "high"}), "accepted_set_priority") \
    .add(dqm.min('numViews'), "min_views") \

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()
