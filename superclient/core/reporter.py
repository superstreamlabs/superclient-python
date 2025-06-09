"""Client reporting functionality."""

import json
from typing import Any, Dict

from ..logger import get_logger
from ..model.messages import ClientMessage, ClientStatsMessage
from ..util.config import copy_security, mask_sensitive
from ..util.network import get_host_info

logger = get_logger("core.reporter")

_SUPERLIB_PREFIX = "superstreamlib-"

def internal_send_clients(bootstrap: str, base_cfg: Dict[str, Any], payload: bytes) -> None:
    """Send payload to superstream.clients using available Kafka library."""
    # Attempt kafka-python first
    try:
        import kafka  # type: ignore

        cfg = {
            "bootstrap_servers": bootstrap,
            "client_id": _SUPERLIB_PREFIX + "client-reporter",
            "compression_type": "zstd",
            "batch_size": 16_384,
            "linger_ms": 1000,
        }
        copy_security(base_cfg, cfg)
        prod = kafka.KafkaProducer(**{k.replace(".", "_"): v for k, v in cfg.items()})
        prod.send("superstream.clients", payload)
        prod.flush()
        prod.close()
        return
    except Exception:
        pass  # fallthrough

    # Fallback to confluent-kafka if available
    try:
        from confluent_kafka import Producer as _CProducer  # type: ignore

        cfg = {
            "bootstrap.servers": bootstrap,
            "client.id": _SUPERLIB_PREFIX + "client-reporter",
            "compression.type": "zstd",
            "batch.size": 16384,
            "linger.ms": 1000,
        }
        copy_security(base_cfg, cfg)
        prod = _CProducer(cfg)
        prod.produce("superstream.clients", payload)
        prod.flush()
    except Exception:
        # As a last resort just log and drop – should never interrupt app
        logger.debug("Failed to send clients message via all libraries")

def send_clients_msg(tracker: Any, error: str = "") -> None:
    """Send a message to the clients topic."""
    hostname, ip = get_host_info()
    msg = ClientStatsMessage(
        client_id=tracker.client_id,
        ip_address=ip,
        topics=sorted(tracker.topics),
        original_configuration={k: mask_sensitive(k, v) for k, v in tracker.orig_cfg.items()},
        optimized_configuration={k: mask_sensitive(k, v) for k, v in tracker.opt_cfg.items()},
        environment_variables={k: v for k, v in os.environ.items() if k.startswith("SUPERSTREAM_")},
        hostname=hostname,
        superstream_client_uid=tracker.uuid,
        most_impactful_topic=tracker.determine_topic(),
        error=error,
    )
    payload = json.dumps(msg.__dict__).encode()
    internal_send_clients(tracker.bootstrap, tracker.orig_cfg, payload)
    logger.debug("Sent clients message for {}", tracker.client_id) 