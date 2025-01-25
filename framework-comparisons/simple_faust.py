import time
import faust
from config.configurations import (
    kafka_topic, kafka_server,
    directory_base_name, faust_directory
)
from utils import is_out_of_range as range_validator, set_up_output_path, standardize_timestamp_to_nanoseconds

RESULTS_OUTPUT_FILE = directory_base_name + faust_directory + 'results.csv'
set_up_output_path(RESULTS_OUTPUT_FILE)

app = faust.App(
    'simple_dq',
    broker=kafka_server,
)


@app.agent()
async def write_to_csv(stream):
    with open(RESULTS_OUTPUT_FILE, 'a') as csvfile:
        async for value in stream:
            csvfile.write(
                f'{value[0]},'  # current number of missing values
                f'{value[1]},'  # current number of values out of range
                f'{value[2]}'  # timestamp that the message was emitted
                f'\n')


data_topic = app.topic(kafka_topic)


@app.agent(data_topic, sink=[write_to_csv])
async def monitor_stream(messages):
    missing_values = out_of_range_values = 0
    async for message in messages:
        value = message['value']

        is_missing = value is None
        is_out_of_range = range_validator(value)

        if is_missing:
            missing_values += 1
        if is_out_of_range:
            out_of_range_values += 1

        if is_missing or is_out_of_range:
            yield (
                missing_values,
                out_of_range_values,
                standardize_timestamp_to_nanoseconds(time.time())
            )
