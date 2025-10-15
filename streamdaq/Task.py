from datetime import timedelta
from typing import Any, Callable, Optional, Self, TYPE_CHECKING
from collections import OrderedDict

import pathway as pw
from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

from streamdaq.artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial
from streamdaq.utils import create_comparison_function, extract_violation_count
from streamdaq.SchemaValidator import SchemaValidator
from streamdaq.CompactData import CompactData

# This is for improved developer experience using type annotations
# See https://tinyurl.com/4cycs6jn for reference
if TYPE_CHECKING:
    from streamdaq.StreamDaQ import StreamDaQ


class Task:
    """
    A Task represents a complete data quality monitoring configuration for a single data source.
    Each task encapsulates its own source, windowing, measures, schema validation, and output handling.
    Tasks can operate independently within a StreamDaQ instance, allowing multi-source monitoring
    with different configurations per source.
    """

    # -- Internal state keys

    # ---- Compact data configurations (if any)
    _FIELDS_KEY: str = "FIELDS"
    _VALUES_KEY: str = "VALUES"

    # ---- Checks assigned to this task
    _CHECKS_KEY: str = "CHECKS"

    # If we decide to migrate to RocksDB, we can probably add more keys in state

    def __init__(self, name: str, critical: bool = False):
        """
        Initialize a new monitoring task. **The name must be unique**

        :param name: Unique name for this task
        :param critical: If True, failure of this task will stop all monitoring
        """

        # State management at the task level
        self._TASK_INTERNAL_STATE = None
        self.__initialize_state()

        # User-driven parameters
        self.name = name
        self.critical = critical

        # Internal counter to consistently handle unnamed checks
        self._check_counter = 0

        # Core monitoring configuration (moved from StreamDaQ)
        self.task_output = OrderedDict()
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
        self.schema_validator = None
        self.compact_data = None

    def __initialize_state(self) -> None:
        self._TASK_INTERNAL_STATE = dict()
        checks: OrderedDict[str, pw.ColumnExpression | ReducerExpression] = OrderedDict()
        self._TASK_INTERNAL_STATE[self._CHECKS_KEY] = checks

    @property
    def _checks(self) -> OrderedDict:
        return self._TASK_INTERNAL_STATE[self._CHECKS_KEY]

    def _generate_check_name(self):
        while True:
            self._check_counter += 1
            check_name = f"Unnamed_check_{self._check_counter}"
            if not check_name in self.task_output:
                return check_name

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
        schema_validator: SchemaValidator | None = None,
        compact_data: CompactData | None = None,
    ) -> Self:
        """
        Configure the data quality monitoring parameters for this task.

        :param window: a window object to use for windowing the source stream
        :param time_column: the name of the column that contains the date/time information for every element
        :param behavior: the temporal behavior of the monitoring window; see pathways temporal behaviors for more
        :param instance: the name of the column that contains the key for each incoming element
        :param wait_for_late: the number of seconds to wait after the end timestamp of each window
        :param time_format: the format of the values in the column that contains date/time information
        :param show_window_start: boolean flag to specify whether the window starting timestamp should be included
        :param show_window_end: boolean flag to specify whether the window ending timestamp should be included
        :param source: the source to get data from
        :param sink_file_name: the name of the file to write the output to
        :param sink_operation: the operation to perform in order to send data out of Stream DaQ
        :param schema_validator: an optional schema validator to apply on the input data stream
        :param compact_data: an optional compact data configuration for working with compact data representations
        :return: a self reference for method chaining
        """
        self.window = window
        self.window_behavior = behavior
        self.instance = instance
        self.time_column = time_column
        self.wait_for_late = wait_for_late
        self.time_format = time_format

        if self.instance:
            self.task_output[self.instance] = pw.reducers.any(pw.this[self.instance])

        self.show_window_start = show_window_start
        self.show_window_end = show_window_end
        if self.show_window_start:
            self.task_output["window_start"] = pw.this._pw_window_start
        if self.show_window_end:
            self.task_output["window_end"] = pw.this._pw_window_end

        self.source = source
        self.sink_file_name = sink_file_name
        self.sink_operation = sink_operation
        self.schema_validator = schema_validator
        self.compact_data = compact_data
        return self

    def check(
        self,
        measure: pw.ColumnExpression | ReducerExpression,
        must_be: str | Callable[[Any], bool] | None = None,
        name: Optional[str] = None,
    ) -> Self:
        """
        Add a data quality check to be monitored within the stream windows.

        :param measure: the measure to be monitored
        :param must_be: the assessment mechanism to be applied on the measure. Can be a callable or a string, e.g., ">4"
        :param name: the name with which the measure and assessment result will appear in the output
        :return: a self reference for method chaining
        """
        if not name:
            name = self._generate_check_name()

        if not must_be:
            self.task_output[name] = measure
            return self

        assessment_function = must_be if callable(must_be) else create_comparison_function(must_be)
        assessment_result = pw.apply_with_type(assessment_function, bool, measure)
        self.task_output[name] = pw.apply_with_type(tuple, tuple, (measure, assessment_result))
        return self

    def _get_data_source_or_else_artificial(self) -> pw.Table:
        """Get data source, falling back to artificial if none specified."""
        if self.source:
            return self.source
        
        data = artificial(number_of_rows=100, input_rate=10).with_columns(
            date_and_time=pw.this.timestamp.dt.strptime(self.time_format),
            timestamp=pw.cast(float, pw.this.timestamp),
        )
        self.compact_data = None  # The artificial data source is native
        print(f"Task '{self.name}': No source provided. Data set to artificial and format to native.")
        return data

    def _convert_to_native_if_needed(self, data: pw.Table) -> pw.Table:
        """Convert compact data to native format if needed."""
        if not self.compact_data:
            return data

        fields_column = self.compact_data._fields_column
        values_column = self.compact_data._values_column
        values_dtype = self.compact_data._values_dtype
        type_exceptions = self.compact_data._type_exceptions

        def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
            if self._FIELDS_KEY in self._TASK_INTERNAL_STATE:
                # TODO: here we can detect and handle schema evolution!
                return
            self._TASK_INTERNAL_STATE[self._FIELDS_KEY] = row[fields_column]
            # self._DAQ_INTERNAL_STATE[self._FIELDS_KEY] now contains the field names
            # e.g., ['temperature', 'pressure']

        pw.io.subscribe(table=data, on_change=on_change)
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

        column_names = self._TASK_INTERNAL_STATE[self._FIELDS_KEY]
        compact_to_native_transformations = {column_names[i]: pw.this.values.get(i) for i in range(len(column_names))}
        native_data_types = {column_names[i]: values_dtype for i in range(len(column_names))}
        
        # overwrite the default types with the user-specified exceptions, if any
        if type_exceptions:
            for column_name, dtype in type_exceptions.items():
                if column_name in column_names:
                    native_data_types[column_name] = dtype

        return (
            data.with_columns(**compact_to_native_transformations)
            .update_types(**native_data_types)
            .without(fields_column, values_column)
        )

    def _validate_schema_if_needed(self, data: pw.Table) -> pw.Table:
        """If schema validation is configured, enriches data with schema validation metadata."""
        if not self.schema_validator:
            return data
        return self.schema_validator.validate_data_stream(data)

    def _deflect_violations_if_needed(self, data: pw.Table) -> pw.Table | None:
        """Handles deflection of records violating schema constraints."""
        if not self.schema_validator:
            return None
        if not self.schema_validator.settings().deflect_violating_records:
            return None
        return data.filter(pw.this._validation_metadata[0] == False)

    def _keep_compliant_data_if_needed(self, data: pw.Table) -> pw.Table:
        """Handles filtering of records compliant to schema constraints."""
        if not self.schema_validator:
            return data
        if not self.schema_validator.settings().filter_respecting_records:
            return data
        return data.filter(pw.this._validation_metadata[0] == True)

    def _window_measure_and_assess(self, data: pw.Table) -> pw.Table:
        """Apply windowing and compute measures and assessments."""
        if self.schema_validator:
            column_name = self.schema_validator.settings().column_name
            data = data.with_columns(
                **{column_name: pw.apply_with_type(extract_violation_count, int, pw.this._validation_metadata[1])},
                error_messages=pw.apply_with_type(
                    lambda x: None if x == "" else x, str | None, pw.this._validation_metadata[1]
                ),
            )
            self.task_output[self.schema_validator.settings().column_name] = pw.reducers.sum(pw.this[column_name])
            self.task_output["error_messages"] = pw.reducers.tuple(pw.this.error_messages, skip_nones=True)

        return data.windowby(
            data[self.time_column],
            window=self.window,
            instance=data[self.instance] if self.instance else None,
            behavior=self.window_behavior or pw.temporal.exactly_once_behavior(shift=self.wait_for_late),
            # TODO (Vassilis) handle the case int | timedelta (in another PR)
        ).reduce(**self.task_output)

    def _raise_alerts_if_needed(self, data: pw.Table) -> pw.Table | None:
        """Raises alerts only if alerting behavior is configured."""
        # if schema validation is not set, no action needed
        if not self.schema_validator:
            return None

        # if neither logging nor exception raising is set for violations, no action needed
        validation_settings = self.schema_validator.settings()
        if not validation_settings.log_violations and not validation_settings.raise_on_violation:
            return None

        return self.schema_validator.raise_alerts(data)

    def _remove_error_messages_if_needed(self, quality_meta_stream: pw.Table) -> pw.Table:
        """Removes the error_messages column from the quality meta-stream if configured."""
        if not self.schema_validator:
            return quality_meta_stream
        if self.schema_validator.settings().include_error_messages:
            return quality_meta_stream

        cols_to_keep = {col: pw.this[col] for col in quality_meta_stream.column_names() if col != "error_messages"}
        return quality_meta_stream.select(**cols_to_keep)

    def _send_to_sinks_if_needed(
        self, quality_meta_stream: pw.Table, violations: pw.Table | None, alerts: pw.Table | None
    ) -> None:
        """Sends the quality meta-stream, violations, and alerts to configured sinks."""
        # sink for quality meta-stream (always needed) - defaults to console
        quality_meta_stream_sink = self.sink_operation or pw.debug.compute_and_print
        quality_meta_stream_sink(quality_meta_stream)

        # sink for violations (aka deflected stream) if needed - defaults to console
        if violations:
            violations_sink = self.schema_validator.settings().deflection_sink or pw.debug.compute_and_print
            violations_sink(violations)
            print(f"Task '{self.name}': Violations sink has been successfully set.")

        # sink for alerts if needed - defaults to console (as dict objects)
        if alerts:
            pw.debug.table_to_dicts(alerts)

    def execute_monitoring_pipeline(self) -> pw.Table:
        """
        Execute the complete monitoring pipeline for this task.

        :return: The quality meta-stream table for this task
        """
        try:
            data = self._get_data_source_or_else_artificial()
            data = self._convert_to_native_if_needed(data)
            data = self._validate_schema_if_needed(data)
            deflected_data = self._deflect_violations_if_needed(data)
            data = self._keep_compliant_data_if_needed(data)
            quality_meta_stream = self._window_measure_and_assess(data)
            alerts = self._raise_alerts_if_needed(quality_meta_stream)
            quality_meta_stream = self._remove_error_messages_if_needed(quality_meta_stream)

            self._send_to_sinks_if_needed(
                quality_meta_stream=quality_meta_stream, violations=deflected_data, alerts=alerts
            )

            return quality_meta_stream

        except Exception as e:
            if self.critical:
                raise CriticalTaskFailureError(self.name, e)
            else:
                print(f"Task '{self.name}' failed with error: {e}")
                # For non-critical tasks, we could return None or a dummy table
                # For now, we'll re-raise to let the orchestrator handle it
                raise


class CriticalTaskFailureError(Exception):
    """Exception raised when a critical task fails."""

    def __init__(self, task_name: str, original_error: Exception):
        self.task_name = task_name
        self.original_error = original_error
        super().__init__(f"Critical task '{task_name}' failed: {original_error}")
