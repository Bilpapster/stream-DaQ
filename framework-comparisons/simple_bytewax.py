import json
import time
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSink
from bytewax.connectors.kafka import operators as kop, KafkaSourceMessage
from config.configurations import (
    kafka_topic, kafka_server,
    directory_base_name, bytewax_directory,
    missing_values_file_name, out_of_range_values_file_name
)
from utils import is_out_of_range, standardize_timestamp_to_nanoseconds, set_up_output_path
from confluent_kafka import OFFSET_BEGINNING

MISSING_VALUES_OUTPUT_FILE = directory_base_name + bytewax_directory + missing_values_file_name
set_up_output_path(MISSING_VALUES_OUTPUT_FILE)
OUT_OF_RANGE_OUTPUT_FILE = directory_base_name + bytewax_directory + out_of_range_values_file_name
set_up_output_path(OUT_OF_RANGE_OUTPUT_FILE)

MISSING_KEYWORD = 'MISSING'
OUT_OF_RANGE_KEYWORD = 'OUT_OF_RANGE'

rdkafka_settings = {
    "bootstrap.servers": kafka_server,
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}


def extract_value_from_kafka_message(message: KafkaSourceMessage):
    return json.loads(message.value.decode('utf-8'))['value']


def count_elements_in_stream(running_count, _item):
    if running_count is None:
        running_count = 0
    running_count += 1
    finish_processing_timestamp = time.time()
    finish_processing_timestamp = standardize_timestamp_to_nanoseconds(finish_processing_timestamp)
    return running_count, f"{running_count},{finish_processing_timestamp}"


# set up a dataflow and configure kafka input stream
flow = Dataflow("simple_dq")
raw_input_stream = kop.input("kafka-in", flow, brokers=[kafka_server], topics=[kafka_topic], starting_offset=OFFSET_BEGINNING, batch_size=10)
values = op.map("extract_value_from_kafka_message", raw_input_stream.oks, extract_value_from_kafka_message)

# count missing values
missing_values = op.filter("filter_missing_values", values, lambda value: value is None)
missing_values_artificially_keyed = op.key_on("missing_as_key", missing_values, lambda _: MISSING_KEYWORD)
result_missing = op.stateful_map("count_missing_values", missing_values_artificially_keyed, count_elements_in_stream)

# count values out of range
out_of_range_values = op.filter("filter_out_of_range_values", values, is_out_of_range)
out_of_range_values_artificially_keyed = op.key_on("out_of_range_as_key", out_of_range_values,
                                                   lambda _: OUT_OF_RANGE_KEYWORD)
result_out_of_range = op.stateful_map("count_out_of_range_values", out_of_range_values_artificially_keyed,
                                      count_elements_in_stream)

# write results to separate files
op.output("write-missing-values", result_missing, FileSink(MISSING_VALUES_OUTPUT_FILE))
op.output("write-out-of-range-values", result_missing, FileSink(OUT_OF_RANGE_OUTPUT_FILE))

# print results to console (ONLY FOR DEBUGGING)
# op.inspect("missing", result_missing)
# op.inspect("out of range", result_out_of_range)
