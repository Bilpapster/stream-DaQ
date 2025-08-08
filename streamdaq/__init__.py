from .StreamDaQ import StreamDaQ
from .DaQMeasures import DaQMeasures
from .Windows import tumbling, sliding

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    # Python < 3.8 fallback (though you require 3.11+)
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("streamdaq")
except PackageNotFoundError:
    # Package is not installed, fallback to a default
    # This happens during development when running from source
    __version__ = "0.1.8-dev"

# Make version accessible
__all__ = ["StreamDaQ", "DaQMeasures", "Windows", "__version__", "tumbling", "sliding"]
