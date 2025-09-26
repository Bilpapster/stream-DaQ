from datetime import timedelta
from typing import Any, Callable, Optional, Self

import pathway as pw
from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

from streamdaq.artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial
from streamdaq.utils import create_comparison_function, extract_violation_count
from streamdaq.SchemaValidator import SchemaValidator


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
        self.assessments = OrderedDict()
        self.window = None
        self.window_behavior = None
        self.time_column = None
        self.instance = None
        self.wait_for_late = None
        self.time_format = None
        self.show_window_start = True
        self.show_window_end = True
        self.source = None
        self.sink_file_name = None
        self.sink_operation = None
        self.schema_validator = SchemaValidator

    def configure(
            self,
            window: Window,
            time_column: str,
            behavior: pw.temporal.CommonBehavior | pw.temporal.ExactlyOnceBehavior | None = None,
            instance: str | None = None,
            wait_for_late: int | float | timedelta | None = None,
            time_format: str = "%Y-%m-%d %H:%M:%S",
            show_window_start: bool = True,
            show_window_end: bool = True,
            source: pw.internals.Table | None = None,
            sink_file_name: str = None,
            sink_operation: Callable[[pw.internals.Table], None] | None = None,
            schema_validator: SchemaValidator = None
    ) -> Self:
        """
        Configures the DQ monitoring parameters. Specifying a window object, the key instance and the time column name
        cannot be omitted. The rest of the arguments are optional and come with rational default values.
        :param window: a window object to use for widowing the source stream.
        :param time_column: the name of the column that contains the date/time information for every element.
        :param behavior: the temporal behavior of the monitoring window; see pathways temporal behaviors for more.
        :param instance: the name of the column that contains the key for each incoming element.
        :param wait_for_late: the number of seconds to wait after the end timestamp of each window. Late elements that
        arrive more than `wait_for_late` seconds after the window is closed will be ignored.
        :param time_format: the format of the values in the column that contains date/time information
        :param show_window_start: boolean flag to specify whether the window starting timestamp should be included in
        the results
        :param show_window_end: boolean flag to specify whether the window ending timestamp should be included in
        the results
        :param source: the source to get data from.
        :param sink_file_name: the name of the file to write the output to
        :param sink_operation: the operation to perform in order to send data out of Stream DaQ, e.g., a Kafka topic.
        :param schema_validator:
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        self.window = window
        self.window_behavior = behavior
        self.instance = instance
        self.time_column = time_column
        self.wait_for_late = wait_for_late
        self.time_format = time_format

        if self.instance:
            self.measures[self.instance] = pw.reducers.any(pw.this[self.instance])
            self.assessments[self.instance] = pw.reducers.any(pw.this[self.instance])

        self.show_window_start = show_window_start
        self.show_window_end = show_window_end
        if self.show_window_start:
            self.measures["window_start"] = pw.this._pw_window_start
            self.assessments["window_start"] = pw.this._pw_window_start
        if self.show_window_end:
            self.measures["window_end"] = pw.this._pw_window_end
            self.assessments["window_end"] = pw.this._pw_window_end

        self.source = source
        self.sink_file_name = sink_file_name
        self.sink_operation = sink_operation
        self.schema_validator = schema_validator
        return self

    def add(
            self,
            measure: pw.ColumnExpression | ReducerExpression,
            assess: str | Callable[[Any], bool] | None = None,
            name: Optional[str] = None,
    ) -> Self:
        """
        Adds a DQ measurement to be monitored within the stream windows.
        :param measure: the measure to be monitored
        :param assess: the assessment mechanism to be applied on the measure
        :param name: the name with which the measure and assessment result will appear in the output
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        if not name:
            import random

            name = f"Unnamed{random.randint(0, int(1e6))}"
        if not assess:
            self.measures[name] = measure
            return self

        assessment_function = assess if callable(assess) else create_comparison_function(assess)
        assessment_result = pw.apply_with_type(assessment_function, bool, measure)
        self.measures[name] = pw.apply_with_type(tuple, tuple, (measure, assessment_result))
        return self

    def watch_out(self, start: bool = True) -> Optional[pw.Table]:
        """
        Kicks-off the monitoring process. Calling this function at the end of your driver program is necessary, or else
        nothing of what you have declared before will be executed.
        :return: a self reference, so that you can use your favorite, Spark-like, functional syntax :)
        """
        data = self.source
        if self.source is None:  # if no specific input is specified, then fall back to a default dummy stream
            data = artificial(number_of_rows=100, input_rate=10).with_columns(
                date_and_time=pw.this.timestamp.dt.strptime(self.time_format),
                timestamp=pw.cast(float, pw.this.timestamp),
            )
            print("Data set to artificial")

        # Apply schema validation
        if self.schema_validator is not None:
            validated_data = self.schema_validator.validate_data_stream(data)

            # Create stream of raw events for deflected records if needed
            if self.schema_validator.settings().deflect_violating_records:
                deflected_stream = validated_data.filter(pw.this._validation_metadata[0] == False)

            column_args = {col: pw.this[col] for col in validated_data.column_names()}
            validated_data = validated_data.select(
                **column_args,
                schema_errors=pw.apply_with_type(extract_violation_count, int, pw.this._validation_metadata[1]),
                error_messages=pw.apply_with_type(lambda x: None if x == '' else x, str | None,
                                                  pw.this._validation_metadata[1])
            )
            all_measures = {**self.measures,
                            "schema_errors": pw.reducers.tuple(pw.this.error_messages, skip_nones=True)}

            # Windowing and applying all measures including schema violations
            data_measurement = (validated_data.windowby(
                validated_data[self.time_column],
                window=self.window,
                instance=validated_data[self.instance] if self.instance is not None else None,
                behavior=(
                    pw.temporal.exactly_once_behavior(shift=self.wait_for_late)
                    if self.window_behavior is None
                    else self.window_behavior
                ),
                # todo handle the case int | timedelta
            ).reduce(**all_measures))

            # Handle schema violation alerts
            alerts = None
            if self.schema_validator.settings().log_violations or self.schema_validator.settings().raise_on_violation:
                alerts = self.schema_validator.raise_alerts(data_measurement)

            # Apply measurements on valid records only
            if self.schema_validator.settings().filter_respecting_records:
                filtered_data = validated_data.filter(pw.this._validation_metadata[0] == True)
                data_measurement = filtered_data.windowby(
                    filtered_data[self.time_column],
                    window=self.window,
                    instance=validated_data[self.instance] if self.instance is not None else None,
                    behavior=(
                        pw.temporal.exactly_once_behavior(shift=self.wait_for_late)
                        if self.window_behavior is None
                        else self.window_behavior
                    ),
                    # todo handle the case int | timedelta
                ).reduce(**self.measures)

            # Remove error messages from output if not asked
            if not self.schema_validator.settings().include_error_messages:
                cols_to_keep = {col: pw.this[col] for col in data_measurement.column_names() if col != "schema_errors"}
                data_measurement = data_measurement.select(**cols_to_keep)

        else:
            data_measurement = data.windowby(
                data[self.time_column],
                window=self.window,
                instance=data[self.instance] if self.instance is not None else None,
                behavior=(
                    pw.temporal.exactly_once_behavior(shift=self.wait_for_late)
                    if self.window_behavior is None
                    else self.window_behavior
                ),
                # todo handle the case int | timedelta
            ).reduce(**self.measures)


        if start:
            if self.sink_operation is None:
                pw.debug.compute_and_print(data_measurement)
                if self.schema_validator.settings().deflect_violating_records and self.schema_validator.settings().deflection_sink is None:
                    pw.debug.compute_and_print(deflected_stream)

                if alerts is not None:
                    pw.debug.table_to_dicts(alerts) # Force computation to create alerts on the fly - ignore result table
            else:
                self.sink_operation(data_measurement)
                if self.schema_validator is not None and self.schema_validator.settings().deflect_violating_records and self.schema_validator.settings().deflection_sink is not None:
                    self.schema_validator.settings().deflection_sink(deflected_stream)

                if alerts is not None:
                    pw.run_all() # Force computation to create alerts on the fly - ignore result table
                else:
                    pw.run()

        else:
            return data_measurement
