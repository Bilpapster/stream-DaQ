"""
Centralized logging configuration for Stream DaQ.

This module provides a unified logging setup with configurable verbosity levels
for the entire Stream DaQ library. It unifies schema validation logging and other
logging throughout the codebase.
"""

import logging
import sys
from typing import Optional

# Default logging level
_DEFAULT_LEVEL = logging.INFO

# Store the configured level
_current_level: int = _DEFAULT_LEVEL

# Logger name for the entire Stream DaQ library
_LOGGER_NAME = "streamdaq"


def configure_logging(
    level: int | str = logging.INFO,
    format_string: Optional[str] = None,
    date_format: Optional[str] = None,
    handlers: Optional[list] = None,
) -> None:
    """
    Configure logging for the Stream DaQ library.

    This function sets up the logging configuration for all Stream DaQ modules.
    It should be called once at the start of your application to configure
    logging behavior.

    Args:
        level: The logging level. Can be an integer (e.g., logging.INFO) or
               a string (e.g., "INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL").
               Default is logging.INFO.
        format_string: Custom format string for log messages. If None, uses a
                      sensible default format. Default is None.
        date_format: Custom date format for log messages. If None, uses ISO 8601
                    format. Default is None.
        handlers: List of custom logging handlers. If None, uses a StreamHandler
                 writing to sys.stderr. Default is None.

    Example:
        >>> from streamdaq import configure_logging
        >>> import logging
        >>>
        >>> # Set logging to DEBUG level
        >>> configure_logging(level=logging.DEBUG)
        >>>
        >>> # Or use string
        >>> configure_logging(level="WARNING")
        >>>
        >>> # Custom format
        >>> configure_logging(
        ...     level=logging.INFO,
        ...     format_string="%(levelname)s - %(message)s"
        ... )
    """
    global _current_level

    # Convert string level to integer if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    _current_level = level

    # Set default format if not provided
    if format_string is None:
        format_string = "[%(levelname)s] %(name)s: %(message)s"

    # Set default date format if not provided
    if date_format is None:
        date_format = "%Y-%m-%d %H:%M:%S"

    # Set default handlers if not provided
    if handlers is None:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(format_string, date_format))
        handlers = [handler]

    # Get the root logger for streamdaq
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(level)

    # Remove existing handlers
    logger.handlers.clear()

    # Add new handlers
    for handler in handlers:
        logger.addHandler(handler)

    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.

    This function returns a logger that is a child of the main Stream DaQ logger,
    ensuring consistent logging configuration across all modules.

    Args:
        name: The name of the module requesting the logger. Typically __name__.

    Returns:
        A configured logger instance.

    Example:
        >>> from streamdaq.logging_config import get_logger
        >>>
        >>> logger = get_logger(__name__)
        >>> logger.info("This is an info message")
    """
    # Strip the 'streamdaq.' prefix if present to avoid duplication
    if name.startswith("streamdaq."):
        name = name[len("streamdaq.") :]

    return logging.getLogger(f"{_LOGGER_NAME}.{name}")


def get_current_level() -> int:
    """
    Get the current logging level.

    Returns:
        The current logging level as an integer.

    Example:
        >>> from streamdaq import get_current_level
        >>> import logging
        >>>
        >>> level = get_current_level()
        >>> print(f"Current level: {logging.getLevelName(level)}")
    """
    return _current_level


def set_level(level: int | str) -> None:
    """
    Set the logging level for Stream DaQ.

    This is a convenience function that updates the logging level without
    changing other configuration settings.

    Args:
        level: The logging level. Can be an integer (e.g., logging.INFO) or
               a string (e.g., "INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL").

    Example:
        >>> from streamdaq import set_level
        >>> import logging
        >>>
        >>> # Increase verbosity for debugging
        >>> set_level(logging.DEBUG)
        >>>
        >>> # Reduce verbosity
        >>> set_level("ERROR")
    """
    global _current_level

    # Convert string level to integer if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    _current_level = level

    # Update the logger level
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(level)


# Initialize with default configuration on module import
configure_logging(level=_DEFAULT_LEVEL)
