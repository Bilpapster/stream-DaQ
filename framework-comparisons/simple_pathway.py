import time
import pathway as pw
from config.configurations import (
    kafka_topic, kafka_server,
    directory_base_name, pathway_directory,
    missing_values_file_name, out_of_range_values_file_name
)
from utils import is_out_of_range, set_up_output_path, standardize_timestamp_to_nanoseconds


def main():
    # Prepares the output directories and files. Missing and out of range values are written to separate files.
    MISSING_VALUES_OUTPUT_FILE = directory_base_name + pathway_directory + missing_values_file_name
    set_up_output_path(MISSING_VALUES_OUTPUT_FILE)
    OUT_OF_RANGE_OUTPUT_FILE = directory_base_name + pathway_directory + out_of_range_values_file_name
    set_up_output_path(OUT_OF_RANGE_OUTPUT_FILE)

    # Kafka settings to read from the input stream.
    rdkafka_settings = {
        "bootstrap.servers": kafka_server,
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
        value: int | None

    # Create a sample input table from the data read from the Kafka topic.
    input_table = pw.io.kafka.read(
        rdkafka_settings,
        topic=kafka_topic,
        format="json",
        schema=InputSchema,
        autocommit_duration_ms=1,
    )

    # Count the number of missing values
    missing_values_count = (input_table
                            .filter(input_table.value.is_none())
                            .reduce(result=pw.reducers.count()))

    # Count the number of out of range values
    out_of_range_values_count = (input_table
                                 .filter(pw.apply_with_type(is_out_of_range, bool, pw.this.value))
                                 .reduce(result=pw.reducers.count()))

    # Write the results to two separate files
    pw.io.csv.write(missing_values_count, MISSING_VALUES_OUTPUT_FILE)
    pw.io.csv.write(out_of_range_values_count, OUT_OF_RANGE_OUTPUT_FILE)

    # Kick-off execution (Spark-like lazy evaluation)
    pw.run()


if __name__ == '__main__':
    main()
