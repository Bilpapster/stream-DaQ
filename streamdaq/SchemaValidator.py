import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable, Dict, Any, Union, get_type_hints

from pathway import Schema
from pydantic import BaseModel, ValidationError
import pathway as pw

from streamdaq.utils import unpack_schema, construct_error_message


class AlertMode(Enum):
    """
    Enumeration of available alert modes for schema validation: \n
    - **PERSISTENT**:  Always raise alarm on schema violation.
    - **ONLY_ON_FIRST_K**:  Raise alarm on first k windows only.
    - **ONLY_IF**:  Raise alarm when schema is violated and condition holds.
    """
    PERSISTENT = "persistent"
    ONLY_ON_FIRST_K = "only_on_first_k"
    ONLY_IF = "only_if"


@dataclass
class SchemaValidatorConfig:
    """
    Configuration for schema validation.
    """
    schema: BaseModel
    alert_mode: AlertMode
    k_windows: Optional[int] = None  # Used with ONLY_ON_FIRST_K mode
    condition_func: Optional[Callable[[Dict[str, Any]], bool]] = None  # Used with ONLY_IF mode
    log_violations: bool = True
    raise_on_violation: bool = False
    deflect_violating_records: bool = False
    deflection_sink: Optional[Callable[[pw.internals.Table], None]] = None
    filter_respecting_records: bool = False
    include_error_messages: bool = True
    column_name: str = "_schema_errors"  # Column to store validation results


class SchemaValidator:
    """
    Schema validator for streaming data using Pydantic models.
    Validates data streams before windowing and provides configurable alerting mechanisms.
    """
    def __init__(self, config: SchemaValidatorConfig):
        """
        Initialize the schema validator.
        :param config: Schema validation configuration
        """
        self.config = config
        self.window_count = 0
        self.logger = logging.getLogger(__name__)

        # Validate configuration
        if config.alert_mode == AlertMode.ONLY_ON_FIRST_K and config.k_windows is None:
            raise ValueError("k_windows must be specified when using ONLY_ON_FIRST_K alert mode")

        if config.alert_mode == AlertMode.ONLY_IF and config.condition_func is None:
            raise ValueError("condition_func must be specified when using ONLY_IF alert mode")


    def should_alert(self, record: Dict[str, Any]) -> bool:
        """
        Determine if an alert should be raised based on the alert mode.
        :param record: The record being validated
        :return: True if an alert should be raised
        """
        schema_error = record.get('error_messages', None)
        has_violations = bool(schema_error and any(error for error in schema_error if error))

        if self.config.alert_mode == AlertMode.PERSISTENT:
            if has_violations:
                return True
            else:
                return False
        elif self.config.alert_mode == AlertMode.ONLY_ON_FIRST_K:
            if has_violations:
                self.window_count += 1
                should_alert = self.window_count <= self.config.k_windows
                return should_alert
            else:
                # Reset counter when compliance is restored and log if we had consecutive violations
                if self.window_count >= self.config.k_windows:
                    self.logger.info(
                        f"Schema compliance restored after {self.window_count} consecutive windows with violations")
                self.window_count = 0
                return False
        elif self.config.alert_mode == AlertMode.ONLY_IF:
            if has_violations and self.config.condition_func:
                return self.config.condition_func(record)
            else:
                return False

    def validate_data_stream(self, data: pw.Table) -> pw.Table:
        """
        Apply schema validation to input data stream.
        :param data: Input data stream
        :return:  Input data stream with validation results added
        """
        def validate_row(**kwargs) -> tuple[bool, str]:
            """Validate a single row and return validation metadata."""
            # Convert keyword arguments to a dictionary for validation
            row_dict = dict(kwargs)

            try:
                self.config.schema(**row_dict)
                is_valid, error_msg = True, None
            except ValidationError as e:
                is_valid, error_msg = False, str(e)

            return is_valid, construct_error_message(row_dict, error_msg, stream_flag=True) or ""

        # Apply validation to each row using pw.apply with all columns as arguments
        column_args = {col: pw.this[col] for col in data.column_names()}
        validated_data = data.select(
            **column_args,
            _validation_metadata=pw.apply_with_type(
                validate_row,
                tuple[bool, str],
                **column_args
            )
        )

        return validated_data

    def settings(self) -> SchemaValidatorConfig:
        """
        Retrieve the current schema validation configuration.
        Returns:
            SchemaValidatorConfig: The configuration object containing schema
            validation parameters.
        """
        return self.config

    def create_pw_schema(self) -> type[Schema]:
        """
        Generate a Pathway schema from the configured Pydantic schema.

        Returns:
            pw.Schema: A Pathway schema constructed from the configured
            Pydantic schema's type hints.
        """
        pydantic_schema = get_type_hints(self.config.schema)
        raw_dict_schema = {k: unpack_schema(v) for k, v in pydantic_schema.items()}
        pw_schema = pw.schema_from_types(**{k: v for k, v in raw_dict_schema.items()})
        return pw_schema

    def raise_alerts(self, data: pw.Table) -> pw.Table:
        """
        Raise alerts based on schema validation results and configured alert mode.
        Args:
            data: Processed stream with schema validation metadata.
        Returns:
            pw.Table: A table containing an `is_valid` column indicating whether
            an alert was raised for each record based on the configured alert mode
            and validation results. It's a dummy table just to force the computation into the function.

        """
        def alert_if_needed(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> bool:
            alert = self.should_alert(row) # Check if alert should be raised based on alert mode
            if not alert:
                return False  # Return False for clarity.

            for error in row.get('error_messages'):
                if not error:
                    continue
                if self.config.raise_on_violation:
                    raise ValueError(f"Schema violation detected: {error}")
                # else would be redundant here imho since we are raising an exception
                self.logger.warning(
                    f"[{row.get('window_start')}] Schema violation detected: {error}"
                )
            return alert

        # Traverse the data stream and apply alerting logic
        pw.io.subscribe(data, on_change=alert_if_needed,sort_by=[data.window_start])

        return data

def create_schema_validator(
    schema: BaseModel,
    alert_mode: Union[AlertMode, str] = AlertMode.PERSISTENT,
    k_windows: Optional[int] = None,
    condition_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
    log_violations: bool = True,
    raise_on_violation: bool = False,
    deflect_violating_records: bool = False,
    deflection_sink: Optional[Callable[[pw.internals.Table], None]] = None,
    filter_respecting_records: bool = False,
    include_error_messages: bool = True,
    column_name: str = "_schema_errors"
) -> SchemaValidator:
    """
    Factory function to create a schema validator with simplified parameters.
    :param schema: Pydantic model to validate against
    :param alert_mode: Alert mode (persistent, only_on_first_k, only_if)
    :param k_windows: Number of windows for ONLY_ON_FIRST_K mode
    :param condition_func: Condition function for ONLY_IF mode
    :param log_violations: Whether to log validation violations
    :param raise_on_violation: Whether to raise exceptions on violations
    :param deflect_violating_records: Whether to deflect violating records to a separate stream
    :param deflection_sink: Deflected records sink function
    :param filter_respecting_records: Whether to filter the respecting records
    :param include_error_messages: Whether to include error messages in output stream
    :param column_name: Column name to store validation results
    :return: Configured SchemaValidator instance
    """
    if isinstance(alert_mode, str):
        alert_mode = AlertMode(alert_mode)

    config = SchemaValidatorConfig(
        schema=schema,
        alert_mode=alert_mode,
        k_windows=k_windows,
        condition_func=condition_func,
        log_violations=log_violations,
        raise_on_violation=raise_on_violation,
        deflect_violating_records = deflect_violating_records,
        deflection_sink=deflection_sink,
        filter_respecting_records = filter_respecting_records,
        include_error_messages = include_error_messages,
        column_name=column_name
    )

    return SchemaValidator(config)