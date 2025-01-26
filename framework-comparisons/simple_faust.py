import time
import faust
from config.configurations import (
    kafka_topic, kafka_server,
    directory_base_name, faust_directory
)
from utils import is_out_of_range as range_validator, set_up_output_path, standardize_timestamp_to_nanoseconds

RESULTS_OUTPUT_FILE = directory_base_name + faust_directory + 'results.csv'
set_up_output_path(RESULTS_OUTPUT_FILE)

MISSING_KEYWORD = 'MISSING'
OUT_OF_RANGE_KEYWORD = 'OUT_OF_RANGE'

app = faust.App(
    'simple_dq',
    broker=kafka_server,
)


@app.agent()
async def write_to_csv(stream_of_timestamps):
    with open(RESULTS_OUTPUT_FILE, 'a') as csvfile:
        async for timestamp in stream_of_timestamps:
            csvfile.write(
                f'{results[MISSING_KEYWORD]},'  # current number of missing values
                f'{results[OUT_OF_RANGE_KEYWORD]},'  # current number of values out of range
                f'{timestamp}'  # timestamp that the message was emitted
                f'\n')


data_topic = app.topic(kafka_topic)
results = app.Table('DQ_results', default=int, partitions=1)


@app.agent(data_topic, sink=[write_to_csv])
async def monitor_stream(messages):
    async for message in messages:
        value = message['value']

        is_missing = value is None
        is_out_of_range = range_validator(value)

        if is_missing:
            results[MISSING_KEYWORD] += 1
        if is_out_of_range:
            results[OUT_OF_RANGE_KEYWORD] += 1

        if is_missing or is_out_of_range:
            yield standardize_timestamp_to_nanoseconds(time.time())
