from quixstreams import Application, State
from quixstreams.sinks.core.csv import CSVSink
from config.configurations import (
    kafka_server, kafka_topic,
    directory_base_name, quix_directory,
    missing_values_file_name, out_of_range_values_file_name
)
from utils import is_out_of_range, set_up_output_path

MISSING_VALUES_OUTPUT_FILE = directory_base_name + quix_directory + missing_values_file_name
set_up_output_path(MISSING_VALUES_OUTPUT_FILE)
OUT_OF_RANGE_OUTPUT_FILE = directory_base_name + quix_directory + out_of_range_values_file_name
set_up_output_path(OUT_OF_RANGE_OUTPUT_FILE)


def count_elements(_, state: State):
    """
    A simple stateful function that counts the number of elements in the incoming stream.
    :param _: the currently processed stream element (automatically passed, but not used - denoted by underscore).
    :param state: the state storing the running count of elements in the stream.
    :return: the running count as a meta-stream, i.e., a stream of running counts up to that moment.
    """
    KEY = 'count'
    state.set(KEY, state.get(KEY, 0) + 1)
    # processing timestamp is not needed here, because quix provides it in the file sink automatically
    return state.get(KEY)


# Initialize the application
app = Application(
    broker_address=kafka_server,
    consumer_group="0",
    auto_offset_reset="earliest",
    commit_interval=0.5
)

# Define a topic with messages in JSON format
input_topic = app.topic(name=kafka_topic, value_deserializer="json")

# Create a StreamingDataFrame from the input topic and extract the actual value from the JSON object
sdf = app.dataframe(input_topic).apply(lambda message: message["value"])

# compute the number of missing values and send it to a csv file
(sdf.filter(lambda value: value is None)
 .apply(count_elements, stateful=True)
 .sink(CSVSink(MISSING_VALUES_OUTPUT_FILE)))

# compute the number of values out of range and send it to a csv file
(sdf.filter(lambda value: is_out_of_range(value))
 .apply(count_elements, stateful=True)
 .sink(CSVSink(OUT_OF_RANGE_OUTPUT_FILE)))

# Run the application
if __name__ == "__main__":
    app.run(sdf)
