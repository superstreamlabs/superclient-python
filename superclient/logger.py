from __future__ import annotations
import os
import sys
import threading
import traceback
from datetime import datetime

__all__ = [
    "get_logger",
    "set_debug_enabled",
]

_PREFIX = "[superstream]"
_debug_enabled_lock = threading.Lock()
_debug_enabled: bool = False


def _detect_debug_from_env() -> bool:
    flag = os.getenv("SUPERSTREAM_DEBUG")
    return str(flag).lower() == "true"


with _debug_enabled_lock:
    _debug_enabled = _detect_debug_from_env()


def set_debug_enabled(enabled: bool) -> None:
    global _debug_enabled
    with _debug_enabled_lock:
        _debug_enabled = bool(enabled)


class _Logger:
    __slots__ = ("_name",)

    def __init__(self, name: str) -> None:
        self._name = name

    def _write(self, level: str, msg: str) -> None:
        formatted = f"{_PREFIX} {level} {self._name}: {msg}"
        stream = sys.stderr if level == "ERROR" else sys.stdout
        try:
            stream.write(formatted + "\n")
            stream.flush()  # Ensure the message is written immediately
        except Exception as e:
            # If we can't write to the stream, try to write to stderr as a last resort
            try:
                sys.stderr.write(f"Failed to write log message: {e}\n")
                sys.stderr.flush()
            except Exception:
                pass  # If we can't even write to stderr, give up

    def info(self, msg: str, *args):
        if args:
            msg = msg.format(*args)
        self._write("INFO", msg)

    def warn(self, msg: str, *args):
        if args:
            msg = msg.format(*args)
        self._write("WARN", msg)

    def error(self, msg: str, *args, exc_info=None):
        if args:
            msg = msg.format(*args)
        
        # If an exception is provided or we're in debug mode, include the stack trace
        if exc_info is not None or _debug_enabled:
            if exc_info is True:
                exc_info = sys.exc_info()
            if exc_info:
                if isinstance(exc_info, BaseException):
                    exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
                elif not isinstance(exc_info, tuple):
                    exc_info = sys.exc_info()
                
                if exc_info[0] is not None:
                    msg = f"{msg}\nException: {exc_info[0].__name__}: {exc_info[1]}\nStack trace:\n{''.join(traceback.format_tb(exc_info[2]))}"
        
        self._write("ERROR", msg)

    def debug(self, msg: str, *args):
        if not _debug_enabled:
            return
        if args:
            msg = msg.format(*args)
        self._write("DEBUG", msg)

    warning = warn


def get_logger(obj):
    name = obj if isinstance(obj, str) else getattr(obj, "__name__", str(obj))
    return _Logger(name) 