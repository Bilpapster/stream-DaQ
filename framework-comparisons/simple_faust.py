import time
import faust
from config.configurations import (
    kafka_topic, kafka_server,
    directory_base_name, faust_directory, results_file_name
)
from utils import is_out_of_range as range_validator, set_up_output_path, standardize_timestamp_to_nanoseconds

# Preparations for the output directory and file. Missing and out of range values are written to the same file.
RESULTS_OUTPUT_FILE = directory_base_name + faust_directory + results_file_name
set_up_output_path(RESULTS_OUTPUT_FILE)

# keywords used as keys in the results table
MISSING_KEYWORD = 'MISSING'
OUT_OF_RANGE_KEYWORD = 'OUT_OF_RANGE'

app = faust.App(
    'simple_dq',
    broker=kafka_server,
)


@app.agent()  # we need an agent here, because table `results` cannot be accessed outside an agent context
async def write_to_csv(stream_of_timestamps):
    """
    A faust Agent that receives a stream of timestamps. Each timestamp in this stream denotes that a change has occurred
    in either the number of missing values, the number of values out of range, or both. On the arrival of every new such
    timestamp, the agent appends a csv line in the format <#missing>,<#out-of-range>,<timestamp>.
    Note that timestamp is standardized to represent nanoseconds for uniformity with the other frameworks to draw safer
    conclusions regarding their comparison.
    :param stream_of_timestamps: a stream of timestamps representing when either the number of missing values, the
     number of values out of range, or both have changed.
    """
    async for timestamp in stream_of_timestamps:
        with open(RESULTS_OUTPUT_FILE, 'a') as csvfile:
            csvfile.write(
                f'{results[MISSING_KEYWORD]},'  # current number of missing values
                f'{results[OUT_OF_RANGE_KEYWORD]},'  # current number of values out of range
                f'{timestamp}'  # timestamp that the message was emitted
                f'\n')


data_topic = app.topic(kafka_topic)  # the topic to read input data from
results = app.Table('DQ_results', default=int, partitions=1)  # a (fault-tolerant) table (state) to store the results


@app.agent(data_topic, sink=[write_to_csv])
async def monitor_stream(messages):
    """
    A faust Agent that processes the incoming data stream and computes two simplistic data quality metrics: missing and
    out of range values. The running values of these metrics are stored (as stream state) in the `results` faust table.
    Whenever a change occurs to any of these metrics, the agent emits the timestamp the processing is finished for the
    current element, forwarding the results to the `write_to_csv` agent, which serves as a sink.
    :param messages: The incoming elements of the input stream, having just an int (nullable) value.
    """
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
