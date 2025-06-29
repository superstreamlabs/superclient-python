"""Metadata handling functionality."""

import json
import os
from typing import Any, Dict, Optional, Literal

from ..util.logger import get_logger
from ..util.config import copy_client_configuration_properties, translate_java_to_lib

logger = get_logger("agent.metadata")

_DEFAULTS = {"compression.type": "zstd", "batch.size": 32_768, "linger.ms": 5_000}

# ---------------------------------------------------------------------------
# Library-specific consumer creation helpers (local to this module)
# ---------------------------------------------------------------------------

def _create_consumer_kafka_python(bootstrap: str, base_cfg: Dict[str, Any]):
    import kafka  # type: ignore

    consumer_cfg = {
        "bootstrap_servers": bootstrap,
        "client_id": "superstreamlib-metadata-consumer",
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
    }
    copy_client_configuration_properties(base_cfg, consumer_cfg)
    return kafka.KafkaConsumer(**consumer_cfg)

def _create_consumer_aiokafka(bootstrap: str, base_cfg: Dict[str, Any]):
    from aiokafka import AIOKafkaConsumer  # type: ignore

    consumer_cfg = {
        "bootstrap_servers": bootstrap,
        "client_id": "superstreamlib-metadata-consumer",
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
    }
    copy_client_configuration_properties(base_cfg, consumer_cfg)
    return AIOKafkaConsumer(**consumer_cfg)

def _create_consumer_confluent(bootstrap: str, base_cfg: Dict[str, Any]):
    from confluent_kafka import Consumer  # type: ignore

    consumer_cfg = {
        "bootstrap.servers": bootstrap,
        "client.id": "superstreamlib-metadata-consumer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    copy_client_configuration_properties(base_cfg, consumer_cfg)
    return Consumer(consumer_cfg)

_CONSUMER_BUILDERS = {
    "kafka-python": _create_consumer_kafka_python,
    "aiokafka": _create_consumer_aiokafka,
    "confluent": _create_consumer_confluent,
}

def fetch_metadata(
    bootstrap: str,
    cfg: Dict[str, Any],
    lib_name: Literal["kafka-python", "aiokafka", "confluent"],
) -> Optional[Dict[str, Any]]:
    """Fetch the latest optimization metadata from the Superstream internal topic.

    The consumer is created using the *same* Kafka library that the application
    itself employs (`lib_name`).  This guarantees compatibility with user
    dependencies and avoids version-related conflicts.
    """

    builder = _CONSUMER_BUILDERS.get(lib_name)
    if builder is None:
        logger.error("[ERR-204] Unsupported Kafka library: {}", lib_name)
        return None

    topic = "superstream.metadata_v1"
    try:
        consumer = builder(bootstrap, cfg)

        if not consumer.partitions_for_topic(topic):
            logger.error(
                "[ERR-201] Superstream internal topic is missing. Please ensure permissions for superstream.* topics."
            )
            consumer.close()
            return None

        if lib_name == "confluent":
            from confluent_kafka import TopicPartition  # type: ignore

            tp = TopicPartition(topic, 0)
            consumer.assign([tp])
            low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
            if high == 0:
                logger.error(
                    "[ERR-202] Unable to retrieve optimizations data from Superstream – topic empty."
                )
                consumer.close()
                return None
            consumer.seek(TopicPartition(topic, 0, high - 1))
            msg = consumer.poll(timeout=5.0)
            consumer.close()
            if msg and msg.value():
                return json.loads(msg.value().decode())

        else:  # kafka-python / aiokafka share similar API
            import kafka as _kafka  # type: ignore

            tp = _kafka.TopicPartition(topic, 0)
            consumer.assign([tp])
            consumer.seek_to_end(tp)
            end = consumer.position(tp)
            if end == 0:
                logger.error(
                    "[ERR-202] Unable to retrieve optimizations data from Superstream – topic empty."
                )
                consumer.close()
                return None
            consumer.seek(tp, end - 1)
            recs = consumer.poll(timeout_ms=5000)
            consumer.close()
            for batch in recs.values():
                for rec in batch:
                    return json.loads(rec.value.decode())
    except Exception as exc:
        logger.error("[ERR-203] Failed to fetch metadata: {}", exc)
    return None

def optimal_cfg(metadata: Optional[Dict[str, Any]], topics: list[str], orig: Dict[str, Any], lib_name: str) -> tuple[Dict[str, Any], str]:
    """Compute optimal configuration based on metadata and topics.
    
    Returns:
        tuple: (configuration_dict, warning_message)
    """
    latency = os.getenv("SUPERSTREAM_LATENCY_SENSITIVE", "false").lower() == "true"
    cfg: Dict[str, Any]
    warning_msg = ""
    
    if not metadata or not metadata.get("topics_configuration"):
        logger.debug("No metadata or topics_configuration found; applying default configuration: %s", _DEFAULTS)
        if not topics:
            warning_msg = "No SUPERSTREAM_TOPICS_LIST environment variable set. Please set it to enable topic-specific optimizations."
        else:
            warning_msg = "The topics you're publishing to haven't been analyzed yet. For optimal results, either wait for the next analysis cycle or trigger one manually via the SuperClient Console"
        logger.warning(warning_msg)
        cfg = dict(_DEFAULTS)
    else:
        matches = [tc for tc in metadata["topics_configuration"] if tc["topic_name"] in topics]
        if not matches:
            logger.debug("No matching topics found in metadata; applying default configuration: %s", _DEFAULTS)
            if not topics:
                warning_msg = "No SUPERSTREAM_TOPICS_LIST environment variable set. Please set it to enable topic-specific optimizations."
            else:
                warning_msg = "The topics you're publishing to haven't been analyzed yet. For optimal results, either wait for the next analysis cycle or trigger one manually via the SuperClient Console"
            logger.warning(warning_msg)
            cfg = dict(_DEFAULTS)
        else:
            best = max(matches, key=lambda tc: tc["potential_reduction_percentage"] * tc["daily_writes_bytes"])
            cfg = dict(best.get("optimized_configuration", {}))
            for k, v in _DEFAULTS.items():
                cfg.setdefault(k, v)
    
    if latency:
        cfg.pop("linger.ms", None)
    
    # Translate Java-style keys to library-specific keys for comparison
    java_keys_to_check = ["batch.size", "linger.ms"]
    lib_keys_to_check = []
    for java_key in java_keys_to_check:
        translated = translate_java_to_lib({java_key: ""}, lib_name)
        lib_key = list(translated.keys())[0] if translated else java_key
        lib_keys_to_check.append(lib_key)
    
    for java_key, lib_key in zip(java_keys_to_check, lib_keys_to_check):
        if lib_key in orig and java_key in cfg:
            try:
                if int(orig[lib_key]) > int(cfg[java_key]):
                    cfg[java_key] = orig[lib_key]
            except Exception:
                pass
    
    # Translate Java-style keys to library-specific keys
    return translate_java_to_lib(cfg, lib_name), warning_msg 