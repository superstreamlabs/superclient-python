"""Client communication functionality."""

import json
import os
from typing import Any, Dict

from ..util.logger import get_logger
from .. import __version__ as _VERSION
from ..util.config import (
    copy_client_configuration_properties,
    convert_to_dot_syntax,
    get_original_config,
    mask_sensitive,
)

logger = get_logger("agent.clients")

_SUPERLIB_PREFIX = "superstreamlib-"

def _create_producer_kafka_python(bootstrap: str, base_cfg: Dict[str, Any]):
    import kafka  # type: ignore

    cfg = {
        "bootstrap.servers": bootstrap,
        "client.id": _SUPERLIB_PREFIX + "client-reporter",
        "compression.type": "zstd",
        "batch.size": 16_384,
        "linger.ms": 1000,
    }
    copy_client_configuration_properties(base_cfg, cfg)
    kafka_cfg = {k.replace(".", "_"): v for k, v in cfg.items()}
    return kafka.KafkaProducer(**kafka_cfg)


def _create_producer_confluent(bootstrap: str, base_cfg: Dict[str, Any]):
    from confluent_kafka import Producer as _CProducer  # type: ignore

    cfg = {
        "bootstrap.servers": bootstrap,
        "client.id": _SUPERLIB_PREFIX + "client-reporter",
        "compression.type": "zstd",
        "batch.size": 16384,
        "linger.ms": 1000,
    }
    copy_client_configuration_properties(base_cfg, cfg)
    return _CProducer(cfg)


_PRODUCER_BUILDERS = {
    "kafka-python": _create_producer_kafka_python,
    "confluent": _create_producer_confluent,
    # aiokafka is async; for heartbeat reporting we use kafka-python builder as fallback
    "aiokafka": _create_producer_kafka_python,
}


def internal_send_clients(
    bootstrap: str,
    base_cfg: Dict[str, Any],
    payload: bytes,
    lib_name: str,
) -> None:
    """Send payload to superstream.clients using the *same* Kafka library as the application.

    In the unlikely event of failure we silently swallow exceptions – reporting
    must never interrupt the user application – but we log for troubleshooting.
    """

    builder = _PRODUCER_BUILDERS.get(lib_name)
    if builder is None:
        logger.debug("Unknown Kafka library '{}', skipping client report", lib_name)
        return

    try:
        prod = builder(bootstrap, base_cfg)
        # confluent-kafka Producer uses .produce, kafka-python uses .send
        if hasattr(prod, "produce"):
            prod.produce("superstream.clients", payload)
            prod.flush()
        else:
            prod.send("superstream.clients", payload)
            prod.flush()
            prod.close()
    except Exception:
        logger.debug("Failed to send clients message via {}", lib_name)

def get_host_info() -> tuple[str, str]:
    """Get hostname and IP address."""
    import socket
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except Exception:
        ip = ""
    return hostname, ip

def send_clients_msg(tracker: Any, error: str = "") -> None:
    """Send a message to the clients topic."""
    hostname, ip = get_host_info()
    # Prepare configuration dictionaries in dot syntax with defaults merged
    orig_cfg_dot = get_original_config(tracker.orig_cfg, tracker.library)
    orig_cfg_masked = {k: mask_sensitive(k, v) for k, v in orig_cfg_dot.items()}

    opt_cfg_dot = convert_to_dot_syntax(tracker.opt_cfg, tracker.library)
    opt_cfg_masked = {k: mask_sensitive(k, v) for k, v in opt_cfg_dot.items()}

    msg_dict = {
        "client_id": tracker.client_id,
        "ip_address": ip,
        "type": "producer",
        "message_type": "client_stats",
        "version": _VERSION,
        "topics": sorted(tracker.topics),
        "original_configuration": orig_cfg_masked,
        "optimized_configuration": opt_cfg_masked,
        "environment_variables": {k: v for k, v in os.environ.items() if k.startswith("SUPERSTREAM_")},
        "hostname": hostname,
        "superstream_client_uid": tracker.uuid,
        "most_impactful_topic": tracker.determine_topic(),
        "language": f"Python ({tracker.library})",
        "error": error,
    }
    payload = json.dumps(msg_dict).encode()
    internal_send_clients(tracker.bootstrap, tracker.orig_cfg, payload, tracker.library)
    logger.debug("Sent clients message for {}", tracker.client_id) 