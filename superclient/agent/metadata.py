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
    copy_client_configuration_properties(base_cfg, consumer_cfg, "kafka-python")
    return kafka.KafkaConsumer(**consumer_cfg)

def _create_consumer_aiokafka(bootstrap: str, base_cfg: Dict[str, Any]):
    from aiokafka import AIOKafkaConsumer  # type: ignore

    consumer_cfg = {
        "bootstrap_servers": bootstrap,
        "client_id": "superstreamlib-metadata-consumer",
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
    }
    copy_client_configuration_properties(base_cfg, consumer_cfg, "aiokafka")
    return AIOKafkaConsumer(**consumer_cfg)

def _create_consumer_confluent(bootstrap: str, base_cfg: Dict[str, Any]):
    from confluent_kafka import Consumer  # type: ignore

    consumer_cfg = {
        "bootstrap.servers": bootstrap,
        "client.id": "superstreamlib-metadata-consumer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    copy_client_configuration_properties(base_cfg, consumer_cfg, "confluent")
    return Consumer(consumer_cfg)

_CONSUMER_BUILDERS = {
    "kafka-python": _create_consumer_kafka_python,
    "aiokafka": _create_consumer_aiokafka,
    "confluent": _create_consumer_confluent,
}

async def fetch_metadata(
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

        elif lib_name == "kafka-python":
            import kafka as _kafka  # type: ignore

            tp = _kafka.TopicPartition(topic, 0)
            consumer.assign([tp])
            
            # Get the end offset safely
            end_offsets = consumer.end_offsets([tp])
            end = end_offsets.get(tp, 0)
            
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

        elif lib_name == "aiokafka":
            # aiokafka uses its own TopicPartition and async API
            from aiokafka import TopicPartition  # type: ignore

            tp = TopicPartition(topic, 0)
            consumer.assign([tp])
            
            # Get the end offset safely using aiokafka's API
            end_offsets = await consumer.end_offsets([tp])
            end = end_offsets.get(tp, 0)
            
            if end == 0:
                logger.error(
                    "[ERR-202] Unable to retrieve optimizations data from Superstream – topic empty."
                )
                consumer.close()
                return None
                
            consumer.seek(tp, end - 1)
            recs = await consumer.getmany(timeout_ms=5000)
            consumer.close()
            for batch in recs.values():
                for rec in batch:
                    return json.loads(rec.value.decode())
    except Exception as exc:
        logger.error("[ERR-203] Failed to fetch metadata: {}", exc)
    return None

def fetch_metadata_sync(
    bootstrap: str,
    cfg: Dict[str, Any],
    lib_name: Literal["kafka-python", "aiokafka", "confluent"],
) -> Optional[Dict[str, Any]]:
    """Synchronous wrapper for fetch_metadata."""
    import asyncio
    
    # Run the async function synchronously for all libraries
    return asyncio.run(fetch_metadata(bootstrap, cfg, lib_name))

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
        # For latency-sensitive applications, don't apply linger.ms optimization
        # Keep the original value if it exists, otherwise use a default
        if "linger.ms" in cfg:
            # Get the original linger.ms value if it exists
            orig_linger_key = None
            if lib_name == "kafka-python" or lib_name == "aiokafka":
                orig_linger_key = "linger_ms"
            elif lib_name == "confluent":
                orig_linger_key = "linger.ms"
            
            if orig_linger_key and orig_linger_key in orig:
                # Use the original value instead of the optimized one
                cfg["linger.ms"] = orig[orig_linger_key]
                logger.debug("Using original linger.ms value ({}) for latency-sensitive application", orig[orig_linger_key])
            else:
                # Remove the optimized value but it will be added back as default later
                cfg.pop("linger.ms")
    
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
                    logger.debug("Keeping original {} value ({}) as it's larger than optimized ({})", java_key, orig[lib_key], cfg[java_key])
                    cfg[java_key] = orig[lib_key]
            except Exception:
                pass
    
    # Ensure all core optimization parameters are present in the final config
    # But don't add linger.ms back if it was removed for latency sensitivity
    for k, v in _DEFAULTS.items():
        if k not in cfg:
            if not (latency and k == "linger.ms"):
                cfg[k] = v
    
    # Translate Java-style keys to library-specific keys
    return translate_java_to_lib(cfg, lib_name), warning_msg 