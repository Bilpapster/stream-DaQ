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
    deflect_violating_records: bool = False,
    deflection_sink: Optional[Callable[[pw.internals.Table], None]] = None,
    filter_respecting_records: bool = False


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
        self.violation_count = 0
        self.window_count = 0
        self.logger = logging.getLogger(__name__)

        # Validate configuration
        if config.alert_mode == AlertMode.ONLY_ON_FIRST_K and config.k_windows is None:
            raise ValueError("k_windows must be specified when using ONLY_ON_FIRST_K alert mode")

        if config.alert_mode == AlertMode.ONLY_IF and config.condition_func is None:
            raise ValueError("condition_func must be specified when using ONLY_IF alert mode")

        if config.deflect_violating_records and config.deflection_sink is None:
            raise ValueError("deflection_sink must be specified when deflecting violating records")

    def validate_record(self, record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate a single record against the schema.
        :param record: Dictionary containing the record to validate
        :return: Tuple of (is_valid, error_message)
        """
        try:
            self.config.schema(**record)
            return True, None
        except ValidationError as e:
            error_msg = str(e)
            return False, error_msg


    def should_alert(self, is_valid: bool, record: Dict[str, Any]) -> bool:
        """
        Determine if an alert should be raised based on the alert mode.
        :param is_valid: Whether the record passed validation
        :param record: The record being validated
        :return: True if an alert should be raised
        """
        if is_valid:
            return False

        self.violation_count += 1

        if self.config.alert_mode == AlertMode.PERSISTENT:
            return True
        elif self.config.alert_mode == AlertMode.ONLY_ON_FIRST_K:
            return self.window_count < self.config.k_windows
        elif self.config.alert_mode == AlertMode.ONLY_IF:
            return self.config.condition_func(record)

        return False

    def process_window_start(self):
        """Called when a new window starts processing."""
        self.window_count += 1

    def validate_data_stream(self, data: pw.Table) -> tuple[pw.Table, pw.Table]:
        """
        Apply schema validation to input data stream.
        :param data: Input data stream
        :return:  Input data stream with validation results added
        """
        def validate_row(**kwargs) -> tuple[bool, str, bool]:
            """Validate a single row and return validation metadata."""
            # Convert keyword arguments to a dictionary for validation
            row_dict = dict(kwargs)

            is_valid, error_msg = self.validate_record(row_dict)
            should_alert = self.should_alert(is_valid, row_dict)

            # Log violations if configured
            if not is_valid and should_alert and self.config.log_violations:
                # self.logger.warning(f"Schema validation failed for record: {error_msg}")
                self.logger.warning(construct_error_message(row_dict, error_msg))

            # Raise exception if configured and should alert
            if not is_valid and should_alert and self.config.raise_on_violation:
                raise ValueError(construct_error_message(row_dict, error_msg))

            return is_valid, construct_error_message(row_dict, error_msg, stream_flag=True) or "", should_alert

        # Apply validation to each row using pw.apply with all columns as arguments
        column_args = {col: pw.this[col] for col in data.column_names()}

        validated_data = data.select(
            **column_args,
            _validation_result=pw.apply_with_type(
                validate_row,
                tuple[bool, str, bool],
                **column_args
            )
        )

        if self.config.deflect_violating_records:
            # Deflect violating records to a separate stream
            deflected_stream = validated_data.filter(
                pw.this._validation_result[2] == True).select(
                **{col: pw.this[col] for col in data.column_names()},
                _schema_error=pw.this._validation_result[1]
            )
            return data, deflected_stream

        if self.config.filter_respecting_records:
            # Keep the respecting records in the input stream
            data = validated_data.filter(
                pw.this._validation_result[0] == True).select(
                **{col: pw.this[col] for col in data.column_names()})


        return data, validated_data

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

def create_schema_validator(
    schema: BaseModel,
    alert_mode: Union[AlertMode, str] = AlertMode.PERSISTENT,
    k_windows: Optional[int] = None,
    condition_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
    log_violations: bool = True,
    raise_on_violation: bool = False,
    deflect_violating_records: bool = False,
    deflection_sink: Optional[Callable[[pw.internals.Table], None]] = None,
    filter_respecting_records: bool = True
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
    :return: Configured SchemaValidator instance

    Args:
        deflection_sink:
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
        filter_respecting_records = filter_respecting_records
    )

    return SchemaValidator(config)