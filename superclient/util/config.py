"""Configuration utility functions."""

import os
from typing import Any, Dict, List

def get_env_vars() -> Dict[str, str]:
    """Get Superstream environment variables."""
    return {k: v for k, v in os.environ.items() if k.startswith("SUPERSTREAM_")}

def is_disabled() -> bool:
    """Check if Superstream is disabled."""
    return os.getenv("SUPERSTREAM_DISABLED", "false").lower() == "true"

def is_debug_enabled() -> bool:
    """Check if debug mode is enabled."""
    return os.getenv("SUPERSTREAM_DEBUG", "false").lower() == "true"

def is_latency_sensitive() -> bool:
    """Check if latency sensitive mode is enabled."""
    return os.getenv("SUPERSTREAM_LATENCY_SENSITIVE", "false").lower() == "true"

def get_topics_list() -> List[str]:
    """Get list of topics from environment variable."""
    return [t.strip() for t in os.getenv("SUPERSTREAM_TOPICS_LIST", "").split(",") if t.strip()]

def mask_sensitive(k: str, v: Any) -> Any:
    """Mask sensitive configuration values."""
    return "[MASKED]" if "password" in k.lower() or "sasl.jaas.config" in k.lower() else v

def copy_security(src: Dict[str, Any], dst: Dict[str, Any]):
    """Copy security-related configuration from source to destination."""
    keys = [
        "security.protocol",
        "sasl.mechanism",
        "sasl.jaas.config",
        "ssl.keystore.password",
        "ssl.truststore.password",
        "ssl.key.password",
        "client.dns.lookup",
    ]
    for k in keys:
        if k in src and k not in dst:
            dst[k] = src[k] 