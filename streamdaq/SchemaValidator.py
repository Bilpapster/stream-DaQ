import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable, Dict, Any, Union

from pydantic import BaseModel, create_model, ValidationError
import pathway as pw


class AlertMode(Enum):
    """
    Enumeration of available alert modes for schema validation.
    """
    PERSISTENT = "persistent"  # Always raise alarm on schema violation
    ONLY_ON_FIRST_K = "only_on_first_k"  # Raise alarm on first k windows only
    ONLY_IF = "only_if"  # Raise alarm when schema is violated and condition holds


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
        Apply schema validation to a data stream.
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
                self.logger.warning(f"Schema validation failed for record: {error_msg}")

            # Raise exception if configured and should alert
            if not is_valid and should_alert and self.config.raise_on_violation:
                raise ValueError(f"Schema validation failed: {error_msg}")

            return is_valid, error_msg or "", should_alert

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
            validated_data = validated_data.filter(
                pw.this._validation_result[0] == False).select(
                **{col: pw.this[col] for col in data.column_names()},
                _schema_error=pw.this._validation_result[1]
            )

        return data, validated_data

    def settings(self) -> SchemaValidatorConfig:
        return self.config



def create_schema_validator(
    schema: BaseModel,
    alert_mode: Union[AlertMode, str] = AlertMode.PERSISTENT,
    k_windows: Optional[int] = None,
    condition_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
    log_violations: bool = True,
    raise_on_violation: bool = False,
    deflect_violating_records: bool = False
) -> SchemaValidator:
    """
    Factory function to create a schema validator with simplified parameters.
    :param schema: Pydantic model to validate against
    :param alert_mode: Alert mode (persistent, only_on_first_k, only_if)
    :param k_windows: Number of windows for ONLY_ON_FIRST_K mode
    :param condition_func: Condition function for ONLY_IF mode
    :param log_violations: Whether to log validation violations
    :param raise_on_violation: Whether to raise exceptions on violations
    :param deflect_violating_records: Whether to raise exceptions on violations
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
        deflect_violating_records = deflect_violating_records
    )

    return SchemaValidator(config)