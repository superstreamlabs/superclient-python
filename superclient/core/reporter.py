"""Client reporting functionality."""

import json
import os
import asyncio
from typing import Any, Dict, List, Optional, Set, Tuple

from ..util.logger import get_logger
from ..model.messages import ClientMessage
from ..util.config import (
    copy_client_configuration_properties,
    translate_lib_to_java,
    get_original_config,
    mask_sensitive,
)
from ..util.network import get_host_info

logger = get_logger("core.reporter")

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


async def _create_producer_aiokafka(bootstrap: str, base_cfg: Dict[str, Any]):
    from aiokafka import AIOKafkaProducer  # type: ignore
    
    cfg = {
        "bootstrap_servers": bootstrap,
        "client_id": _SUPERLIB_PREFIX + "client-reporter",
        "compression_type": "zstd",
        "batch_size": 16_384,
        "linger_ms": 1000,
    }
    copy_client_configuration_properties(base_cfg, cfg)
    return AIOKafkaProducer(**cfg)


_PRODUCER_BUILDERS = {
    "kafka-python": _create_producer_kafka_python,
    "confluent": _create_producer_confluent,
    "aiokafka": _create_producer_aiokafka,
}


def internal_send_clients(bootstrap: str, base_cfg: Dict[str, Any], payload: bytes, lib_name: str) -> None:
    """Send payload to superstream.clients using the library indicated by `lib_name`."""

    builder = _PRODUCER_BUILDERS.get(lib_name)
    if builder is None:
        logger.debug("Unknown Kafka library '{}', skipping client report", lib_name)
        return

    try:
        if lib_name == "aiokafka":
            asyncio.run(internal_send_clients_async(bootstrap, base_cfg, payload))
            return

        prod = builder(bootstrap, base_cfg)
        if hasattr(prod, "produce"):
            prod.produce("superstream.clients", payload)
            prod.flush()
        else:
            prod.send("superstream.clients", payload)
            prod.flush()
            prod.close()
    except Exception:
        logger.debug("Failed to send clients message via {}", lib_name)


async def internal_send_clients_async(bootstrap: str, base_cfg: Dict[str, Any], payload: bytes) -> None:
    """Async version of internal_send_clients for aiokafka."""
    try:
        prod = await _create_producer_aiokafka(bootstrap, base_cfg)
        await prod.start()
        try:
            await prod.send_and_wait("superstream.clients", payload)
        finally:
            await prod.stop()
    except Exception:
        logger.debug("Failed to send clients message via aiokafka")


def send_clients_msg(tracker: Any, error: str = "") -> None:
    """Send a message to the clients topic."""
    hostname, ip = get_host_info()
    orig_cfg_dot = get_original_config(tracker.orig_cfg, tracker.library)
    orig_cfg_masked = {k: mask_sensitive(k, v) for k, v in orig_cfg_dot.items()}

    opt_cfg_dot = translate_lib_to_java(tracker.opt_cfg, tracker.library)
    opt_cfg_masked = {k: mask_sensitive(k, v) for k, v in opt_cfg_dot.items()}

    msg = ClientMessage(
        client_id=tracker.client_id,
        ip_address=ip,
        type="producer",
        message_type="client_stats",
        topics=sorted(tracker.topics),
        original_configuration=orig_cfg_masked,
        optimized_configuration=opt_cfg_masked,
        environment_variables={k: v for k, v in os.environ.items() if k.startswith("SUPERSTREAM_")},
        hostname=hostname,
        superstream_client_uid=tracker.uuid,
        most_impactful_topic=tracker.determine_topic(),
        language=f"Python ({tracker.library})",
        error=error,
    )
    payload = json.dumps(msg.__dict__).encode()
    internal_send_clients(tracker.bootstrap, tracker.orig_cfg, payload, tracker.library)
    logger.debug("Sent clients message for {}", tracker.client_id) 