from datetime import timedelta
from typing import Any, Callable, Optional, Self

import pathway as pw
from pathway.internals import ReducerExpression
from pathway.stdlib.temporal import Window

from streamdaq.artificial_stream_generators import generate_artificial_random_viewership_data_stream as artificial
from streamdaq.utils import create_comparison_function, extract_violation_count
from streamdaq.SchemaValidator import SchemaValidator
from streamdaq.CompactData import CompactData


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

    _FIELDS_KEY: str = "FIELDS"
    _VALUES_KEY: str = "VALUES"

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
        self.schema_validator = None
        self.compact_data = None
        self._DAQ_INTERNAL_STATE = dict()

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
        :param schema_validator: an optional schema validator to apply on the input data stream
        :param compact_data: an optional compact data configuration for working with compact data representations
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
        self.compact_data = compact_data
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

    def _get_data_source_or_else_artificial(self) -> pw.Table:
        """Get data source, falling back to artificial if none specified."""
        if self.source is None:
            data = artificial(number_of_rows=100, input_rate=10).with_columns(
                date_and_time=pw.this.timestamp.dt.strptime(self.time_format),
                timestamp=pw.cast(float, pw.this.timestamp),
            )
            self.compact_data = None  # The artificial data source is native
            print("Data set to artificial and data representation to native.")
            return data
        return self.source

    def _convert_to_native_if_needed(self, data: pw.Table) -> pw.Table:
        if not self.compact_data:
            return data

        fields_column = self.compact_data._fields_column
        values_column = self.compact_data._values_column
        values_dtype = self.compact_data._values_dtype
        type_exceptions = self.compact_data._type_exceptions

        def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
            if self._FIELDS_KEY in self._DAQ_INTERNAL_STATE:
                # TODO: here we can detect and handle schema evolution!
                return
            self._DAQ_INTERNAL_STATE[self._FIELDS_KEY] = row[fields_column]
            # self._DAQ_INTERNAL_STATE[self._FIELDS_KEY] now contains the field names
            # e.g., ['temperature', 'pressure']

        pw.io.subscribe(table=data, on_change=on_change)
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)
        
        column_names = self._DAQ_INTERNAL_STATE[self._FIELDS_KEY]
        compact_to_native_transformations = {
            column_names[i]: pw.this.values.get(i)
            for i in range(len(column_names))
        }
        # at first define all columns to have the 'default' dtype
        native_data_types = {
            column_names[i]: values_dtype
            for i in range(len(column_names))
        }
        # overwrite the default types with the user-specified exceptions, if any
        if type_exceptions:
            for column_name, dtype in type_exceptions.items():
                if column_name in column_names:
                    native_data_types[column_name] = dtype
        
        return data \
            .with_columns(**compact_to_native_transformations) \
            .update_types(**native_data_types) \
            .without(fields_column, values_column)

    def _validate_schema_if_needed(self, data: pw.Table) -> pw.Table:
        """If schema validation is configured, enriches `data`
        with schema validation metadata and returns the enriched stream
        as a `pw.Table`. If schema validation is not configured,
        it performs no action and returns `data` as-is.

        Args:
            data (pw.Table): the stream to validate schema conformance on.

        Returns:
            pw.Table: `data` enriched with schema validation metadata if
            schema validation is not `None`, else `data`.
        """
        if not self.schema_validator:
            return data
        return self.schema_validator.validate_data_stream(data)

    def _deflect_violations_if_needed(self, data: pw.Table) -> pw.Table | None:
        """Handles deflection of records violating schema constraints,
        only if schema validation is configured *AND* deflection of
        violating rows is set to `True` during configuration. If any of
        the above does not hold, it returns `None`.

        Args:
            data (pw.Table): the stream to deflect violations from.

        Returns:
            pw.Table: a separate stream containing only the violating
            records if configured so, else `None`.
        """
        if not self.schema_validator:
            return None
        if not self.schema_validator.settings().deflect_violating_records:
            return None
        return data.filter(pw.this._validation_metadata[0] == False)

    def _keep_compliant_data_if_needed(self, data:pw.Table) -> pw.Table:
        """Handles filtering of records compliant to schema constraints,
        only if schema validation is configured *AND* filtering of
        compliant rows is set to `True` during configuration. If any of
        the above does not hold, it returns `data` as-is.

        Args:
            data (pw.Table): the stream to filter compliant records from.

        Returns:
            pw.Table: a separate stream containing only the compliant
            records if configured so, else `data` without modifications.
        """
        if not self.schema_validator:
            return data
        if not self.schema_validator.settings().filter_respecting_records:
            return data
        return data.filter(pw.this._validation_metadata[0] == True)

    def _window_measure_and_asses(self, data: pw.Table) -> pw.Table:
        if self.schema_validator:
            column_name = self.schema_validator.settings().column_name
            data = data.with_columns(
                **{column_name: pw.apply_with_type(extract_violation_count, int, pw.this._validation_metadata[1])},
                error_messages=pw.apply_with_type(
                    lambda x: None if x == '' else x,
                    str | None,
                    pw.this._validation_metadata[1]
                )
            )
            self.measures[self.schema_validator.settings().column_name] = pw.reducers.sum(pw.this[column_name])
            self.measures['error_messages'] = pw.reducers.tuple(pw.this.error_messages, skip_nones=True)

        return data.windowby(
            data[self.time_column],
            window=self.window,
            instance=data[self.instance] if self.instance else None,
            behavior=self.window_behavior or pw.temporal.exactly_once_behavior(shift=self.wait_for_late),
            # TODO (Vassilis) handle the case int | timedelta (in another PR)
        ).reduce(**self.measures)

    def _raise_alerts_if_needed(self, data: pw.Table) -> pw.Table | None:
        """Raises alerts only if alerting behavior is configured to be
        either logging or exception raising. If neither logging nor
        exception raising is configured, it returns `None`.

        Args:
            data (pw.Table): the stream to raise alerts from.

        Returns:
            pw.Table | None: a separate stream containing alerts if
            the above condition is met, else `None`.
        """
        # if schema validation is not set, no action needed
        if not self.schema_validator:
            return None

        # if neither logging nor exception raising is set for violations, no action needed
        validation_settings = self.schema_validator.settings()
        if not validation_settings.log_violations and \
                not validation_settings.raise_on_violation:
            return None

        return self.schema_validator.raise_alerts(data)

    def _remove_error_messages_if_needed(self, quality_meta_stream: pw.Table) -> pw.Table:
        """Removes the `error_messages` column from the quality meta-stream
        if schema validation is configured *AND* the configuration parameter
        `include_error_messages` is set to `False`. If any of the above
        does not hold, it returns the quality meta-stream as-is.

        Args:
            quality_meta_stream (pw.Table): the quality meta-stream to
            potentially remove the `error_messages` column from.

        Returns:
            pw.Table: the quality meta-stream without the `error_messages`
            column if the above condition is met, else the quality
            meta-stream as-is.
        """
        if not self.schema_validator:
            return quality_meta_stream
        if self.schema_validator.settings().include_error_messages:
            return quality_meta_stream

        cols_to_keep = {col: pw.this[col] for col in quality_meta_stream.column_names() if col != "error_messages"}
        return quality_meta_stream.select(**cols_to_keep)

    def _send_to_sinks_if_needed(self, quality_meta_stream: pw.Table, violations: pw.Table | None,
                                 alerts: pw.Table | None) -> None:
        """Sends the quality meta-stream (always), the violations
        (if configured so) and the alerts (if configured so) to the
        sinks defined at configuration time. For violations and alerts,
        if they are enabled but no sink is specified, the sink defaults
        to the console. For the quality meta-stream, if no sink is
        specified, it is sent to the console as well.

        Args:
            quality_meta_stream (pw.Table): the quality meta-stream to sink (always).
            violations (pw.Table | None): the violations to sink (if needed).
            alerts (pw.Table | None): the alerts to sink (if needed).
        """
        # sink for quality meta-stream (always needed) - defaults to console
        quality_meta_stream_sink = self.sink_operation or pw.debug.compute_and_print
        quality_meta_stream_sink(quality_meta_stream)

        # sink for violations (aka deflected stream) if needed - defaults to console
        if violations:
            print("Violations sink is being set!")
            violations_sink = self.schema_validator.settings().deflection_sink or pw.debug.compute_and_print
            violations_sink(violations)

        # sink for alerts if needed - defaults to console (as dict objects)
        if alerts:
            pw.io.null.write(alerts)  # Dummy sink to force computation


    def watch_out(self, start: bool = True) -> Optional[pw.Table]:
        """
        Kicks-off the data quality monitoring process taking into account the configurations and data quality
        measures that have been defined so far.

        **Important**: All previous configurations are just declaring the different steps of the pipeline.
        Calling `watch_out()` is essential for your declared pipeline to *actually run*.
        This happens when the `start` parameter is set to `True` (default).
        In this case, the function returns `None`.

        **Advanced users** may want to set the `start` parameter to `False`, in order to programmatically
        access the quality meta-stream, comprising data quality measurements and assessment results over time.
        In this case, the function returns a `pw.Table` reference to the quality meta-stream.
        **Users must make sure that `pw.run()` is called** before the end of their script, otherwise the
        defined pipeline will not be executed.

        :return: Does not return (monitoring of unbounded stream) if `start` is set to
        `True` (default), else a `pw.Table` reference to the quality
        meta-stream for advanced programmatic handling (see important notice above).
        """
        data = self._get_data_source_or_else_artificial()
        data = self._convert_to_native_if_needed(data)
        data = self._validate_schema_if_needed(data)
        deflected_data = self._deflect_violations_if_needed(data)
        data = self._keep_compliant_data_if_needed(data)
        quality_meta_stream = self._window_measure_and_asses(data)
        alerts = self._raise_alerts_if_needed(quality_meta_stream)
        quality_meta_stream = self._remove_error_messages_if_needed(quality_meta_stream)

        self._send_to_sinks_if_needed(
            quality_meta_stream=quality_meta_stream,
            violations=deflected_data,
            alerts=alerts
        )

        if not start:
            # ideally `return quality_meta_stream, deflected_data, alerts` but it would be a breaking change - TBD
            return quality_meta_stream

        pw.run_all()
