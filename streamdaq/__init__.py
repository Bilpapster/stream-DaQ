from .DaQMeasures import DaQMeasures
from .StreamDaQ import StreamDaQ
from .Windows import sliding, tumbling
from .SchemaValidator import SchemaValidator, AlertMode, create_schema_validator

try:
    from importlib.metadata import PackageNotFoundError, version
except ImportError:
    # Python < 3.8 fallback (though you require 3.11+)
    from importlib_metadata import PackageNotFoundError, version

try:
    __version__ = version("streamdaq")
except PackageNotFoundError:
    # Package is not installed, fallback to a default
    # This happens during development when running from source
    __version__ = "0.1.8-dev"

# Make version accessible
__all__ = [
    "StreamDaQ", 
    "DaQMeasures", 
    "Windows", 
    "__version__", 
    "tumbling", 
    "sliding", 
    "SchemaValidator", 
    "AlertMode", 
    "create_schema_validator"
]
