from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("superclient")
except PackageNotFoundError:
    # Fallback for when the package isn't installed (e.g. running from source without editable install)
    __version__ = "0.0.0"

# Initialize the Superstream agent on import
from .agent import initialize as _init
_init() 