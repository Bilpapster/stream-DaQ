from StreamDaQ import StreamDaQ
from DaQMeasures import DaQMeasures as dqm
from Windows import tumbling, sliding, session

import os, time
import pathway as pw

PATHWAY_LICENSE_KEY = os.getenv('PATHWAY_LICENSE_KEY', 'You can get yours for free at https://pathway.com/get-license/')
pw.set_license_key(PATHWAY_LICENSE_KEY)


# Get configuration from environment variables
INPUT_KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
OUTPUT_KAFKA_TOPIC = os.getenv('OUTPUT_TOPIC', 'data_output')
READ_FROM_KAFKA_EVERY_MS = os.getenv('READ_FROM_KAFKA_EVERY_MS', '1000')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
SLIDE_DURATION_STR = os.getenv('SLIDE_DURATION', '5 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
MESSAGES_PER_WINDOW_LIST = os.getenv('MESSAGES_PER_WINDOW', '1000')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()

print(f"INPUT_KAFKA_TOPIC: {INPUT_KAFKA_TOPIC}")
print(f"OUTPUT_KAFKA_TOPIC: {OUTPUT_KAFKA_TOPIC}")
print(f"READ_FROM_KAFKA_EVERY_MS: {READ_FROM_KAFKA_EVERY_MS}")
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
            return tumbling(duration=int(parse_duration(WINDOW_DURATION_STR) * 1000),
                            origin=0)  # * 1000 to make it milliseconds
        case 'sliding':
            return sliding(
                duration=int(parse_duration(WINDOW_DURATION_STR) * 1000),
                hop=int(parse_duration(SLIDE_DURATION_STR) * 1000),
                origin=0
            )
        case 'session':
            return session(max_gap=parse_duration(GAP_DURATION_STR) * 1000)
        case _:
            print(f"Unknown window type: {window_str}. Falling back to tumbling.")
            return tumbling(duration=parse_duration(WINDOW_DURATION_STR) * 1000)


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


rdkafka_settings = {
    "bootstrap.servers": KAFKA_SERVER,
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}

data = (pw.io.kafka.read(
    rdkafka_settings,
    topic=INPUT_KAFKA_TOPIC,
    format="json",
    schema=InputSchema,
    autocommit_duration_ms=int(READ_FROM_KAFKA_EVERY_MS),
))


def write_to_kafka(data: pw.internals.Table) -> None:
    pw.io.kafka.write(
        table=data,
        rdkafka_settings=rdkafka_settings,
        topic_name=OUTPUT_KAFKA_TOPIC,
        format="json",
    )


# Step 1: Configure monitoring parameters
daq = StreamDaQ().configure(
    window=get_window_from_string(WINDOW_TYPE),
    time_column="eventTime",
    wait_for_late=0,
    time_format="%Y-%m-%d %H:%M:%S.%f",
    show_window_start=True,
    show_window_end=True,
    source=data,
    sink_operation=write_to_kafka,
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
