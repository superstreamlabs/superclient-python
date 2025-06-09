"""Superstream Python agent (superclient) – see README for details."""

import builtins
import os
import sys
from typing import Any, Dict, Optional, Tuple

from ..logger import get_logger, set_debug_enabled
from ..util.config import get_env_vars, is_disabled
from ..util.network import get_host_info
from .interceptor import patch_kafka_python, patch_aiokafka, patch_confluent
from .tracker import Heartbeat

# ---------------------------------------------------------------------------
# Environment & constants
# ---------------------------------------------------------------------------

_ENV_VARS = get_env_vars()
if os.getenv("SUPERSTREAM_DEBUG", "false").lower() == "true":
    set_debug_enabled(True)

logger = get_logger("agent")
logger.info("Superstream Agent initialized with environment variables: {}", _ENV_VARS)
if is_disabled():
    logger.warn("Superstream functionality disabled via SUPERSTREAM_DISABLED")

_VERSION = "1.0.0"
_original_import = builtins.__import__

# ---------------------------------------------------------------------------
# Module patching
# ---------------------------------------------------------------------------

def _patch_module(module_name: str) -> None:
    """Patch a specific module if it exists in sys.modules."""
    try:
        if module_name == "kafka" and "kafka" in sys.modules:
            patch_kafka_python(sys.modules["kafka"])
        elif module_name == "aiokafka" and "aiokafka" in sys.modules:
            patch_aiokafka(sys.modules["aiokafka"])
        elif module_name == "confluent_kafka" and "confluent_kafka" in sys.modules:
            patch_confluent(sys.modules["confluent_kafka"])
    except Exception as exc:
        logger.error("[ERR-001] Failed to patch {}: {}", module_name, exc)

def _get_base_module(name: str, fromlist: Tuple[str, ...]) -> Optional[str]:
    """Get the base module name for patching.
    
    For example:
    - 'kafka' -> 'kafka'
    - 'kafka.producer' -> 'kafka'
    - 'confluent_kafka' -> 'confluent_kafka'
    - 'confluent_kafka.admin' -> 'confluent_kafka'
    """
    if name in ("kafka", "aiokafka", "confluent_kafka"):
        return name
    parts = name.split(".")
    if parts[0] in ("kafka", "aiokafka", "confluent_kafka"):
        return parts[0]
    return None

def _import_hook(name, globals=None, locals=None, fromlist=(), level=0):
    """Hook into Python's import system to patch Kafka libraries."""
    if is_disabled():
        return _original_import(name, globals, locals, fromlist, level)
        
    module = _original_import(name, globals, locals, fromlist, level)
    try:
        base_module = _get_base_module(name, fromlist)
        if base_module:
            _patch_module(base_module)
    except Exception as exc:
        logger.error("[ERR-002] Failed to patch library {}: {}", name, exc)
    return module

# ---------------------------------------------------------------------------
# Initialise
# ---------------------------------------------------------------------------

def initialize():
    """Initialize the Superstream agent.
    
    This function:
    1. Installs the import hook to catch future imports
    2. Patches any pre-imported modules
    3. Starts the heartbeat thread
    """
    if is_disabled():
        return
        
    # Install import hook
    if builtins.__import__ is not _import_hook:
        builtins.__import__ = _import_hook  # type: ignore
        
    # Patch any pre-imported modules
    for module in ("kafka", "aiokafka", "confluent_kafka"):
        _patch_module(module)
            
    # Start heartbeat
    Heartbeat.ensure() 