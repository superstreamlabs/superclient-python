"""Superstream Python agent (superclient) – see README for details."""

import builtins
import os
import sys
from typing import Any, Dict, Optional, Tuple

from ..util.logger import get_logger, set_debug_enabled
from ..util.config import get_env_vars, is_disabled
from .interceptor import patch_kafka_python, patch_aiokafka, patch_confluent
from .tracker import Heartbeat

# ---------------------------------------------------------------------------
# Environment & constants
# ---------------------------------------------------------------------------

_ENV_VARS = get_env_vars()
if os.getenv("SUPERSTREAM_DEBUG", "false").lower() == "true":
    set_debug_enabled(True)

logger = get_logger("agent")

# Preserve reference to built-in import function
_original_import = builtins.__import__

# ---------------------------------------------------------------------------
# Module patching
# ---------------------------------------------------------------------------

def _patch_module(module_name: str) -> None:
    """Patch a specific module if it exists in sys.modules."""
    try:
        if module_name == "kafka" and "kafka" in sys.modules:
            # Check if KafkaProducer exists before patching
            kafka_module = sys.modules["kafka"]
            if hasattr(kafka_module, "KafkaProducer"):
                patch_kafka_python(kafka_module)
        elif module_name == "aiokafka" and "aiokafka" in sys.modules:
            # Check if AIOKafkaProducer exists before patching
            aiokafka_module = sys.modules["aiokafka"]
            if hasattr(aiokafka_module, "AIOKafkaProducer"):
                patch_aiokafka(aiokafka_module)
        elif module_name == "confluent_kafka" and "confluent_kafka" in sys.modules:
            # Check if Producer exists before patching
            confluent_module = sys.modules["confluent_kafka"]
            if hasattr(confluent_module, "Producer"):
                # Additional check to ensure we can safely patch
                try:
                    patch_confluent(confluent_module)
                except Exception as patch_exc:
                    logger.error("[ERR-003] Failed to patch confluent_kafka Producer: {}", patch_exc)
                    # Don't re-raise, just log the error
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
    2. Schedules patching of any pre-imported modules
    3. Starts the heartbeat thread
    """
    
    # Log initialization message
    logger.info("Superstream Agent initialized with environment variables: {}", _ENV_VARS)
    if is_disabled():
        logger.warn("Superstream functionality disabled via SUPERSTREAM_DISABLED")
    
    if is_disabled():
        return
        
    # Install import hook
    if builtins.__import__ is not _import_hook:
        builtins.__import__ = _import_hook
        
    # Schedule patching of pre-imported modules using a deferred approach
    # This avoids circular import issues by running after the current import completes
    import threading
    def patch_preimported():
        """Patch any modules that were imported before superclient"""
        for module in ("kafka", "aiokafka", "confluent_kafka"):
            if module in sys.modules:
                _patch_module(module)
    
    # Use a timer with 0 delay to run after current import stack completes
    threading.Timer(0, patch_preimported).start()
            
    # Start heartbeat
    Heartbeat.ensure() 