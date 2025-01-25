import pathway as pw
from config.configurations import (
    kafka_topic, kafka_server,
    directory_base_name, pathway_directory,
    missing_values_file_name, out_of_range_values_file_name
)
from utils import is_out_of_range, set_up_output_path


def main():
    MISSING_VALUES_OUTPUT_FILE = directory_base_name + pathway_directory + missing_values_file_name
    set_up_output_path(MISSING_VALUES_OUTPUT_FILE)
    OUT_OF_RANGE_OUTPUT_FILE = directory_base_name + pathway_directory + out_of_range_values_file_name
    set_up_output_path(OUT_OF_RANGE_OUTPUT_FILE)

    rdkafka_settings = {
        "bootstrap.servers": kafka_server,
        "security.protocol": "plaintext",
        "group.id": "0",
        "session.timeout.ms": "6000",
        "auto.offset.reset": "earliest",
    }

    # We define a schema for the table
    # It set all the columns and their types
    class InputSchema(pw.Schema):
        value: int | None

    # Create a sample input table using pw.debug.table_from_markdown()
    input_table = pw.io.kafka.read(
        rdkafka_settings,
        topic=kafka_topic,
        format="json",
        schema=InputSchema,
        autocommit_duration_ms=1,
    )

    # Add columns to identify missing and out-of-range values using with_columns()
    input_table = (input_table
    .with_columns(
        # Check for missing values (only None are considered missing for simplicity)
        is_missing=input_table.value.is_none(),
        # Check for out-of-range values with custom function
        is_out_of_range=pw.apply_with_type(is_out_of_range, bool, pw.this.value),
    ))

    # Count the number of missing values
    missing_values_count_table = input_table.filter(input_table.is_missing).reduce(
        result=pw.reducers.count()
    )

    # Count the number of out-of-range values
    out_of_range_count_table = input_table.filter(input_table.is_out_of_range).reduce(
        total_out_of_range=pw.reducers.count()
    )

    # Write the results to two separate files
    pw.io.csv.write(missing_values_count_table, MISSING_VALUES_OUTPUT_FILE)
    pw.io.csv.write(out_of_range_count_table, OUT_OF_RANGE_OUTPUT_FILE)

    # Kick-off execution (Spark-like lazy evaluation)
    pw.run()


if __name__ == '__main__':
    main()
