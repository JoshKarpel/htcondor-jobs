from typing import Tuple as _Tuple

__version__ = "0.1.0"


def version() -> str:
    """Return a string containing human-readable version information."""
    return f"htcondor-jobs version {__version__}"


def _version_info(v: str) -> _Tuple[int, int, int, str]:
    """Un-format ``__version__``."""
    return (*(int(x) for x in v[:5].split(".")), v[5:])


def version_info() -> _Tuple[int, int, int, str]:
    """Return a tuple of version information: ``(major, minor, micro, release_level)``."""
    return _version_info(__version__)
