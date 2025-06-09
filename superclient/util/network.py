"""Network utility functions."""

import socket
from typing import Tuple

def get_host_info() -> Tuple[str, str]:
    """Get hostname and IP address."""
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except Exception:
        ip = ""
    return hostname, ip 