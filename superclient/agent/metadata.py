"""Metadata handling functionality."""

import json
import os
from typing import Any, Dict, Optional, Literal
import time

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
        "group.id": "superstreamlib-metadata-consumer",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
        "max.poll.interval.ms": 30000,
        "enable.partition.eof": True,
    }
    copy_client_configuration_properties(base_cfg, consumer_cfg, "confluent")
    return Consumer(consumer_cfg)

_CONSUMER_BUILDERS = {
    "kafka-python": _create_consumer_kafka_python,
    "aiokafka": _create_consumer_aiokafka,
    "confluent": _create_consumer_confluent,
}

def fetch_metadata_kafka_python(
    bootstrap: str,
    cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Fetch metadata using kafka-python library."""
    
    builder = _CONSUMER_BUILDERS.get("kafka-python")
    if builder is None:
        logger.error("[ERR-204] Unsupported Kafka library: kafka-python")
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
    except Exception as exc:
        logger.error("[ERR-203] Failed to fetch metadata: {}", exc)
    return None

def fetch_metadata_confluent(
    bootstrap: str,
    cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Fetch metadata using confluent-kafka library."""
    
    builder = _CONSUMER_BUILDERS.get("confluent")
    if builder is None:
        logger.error("[ERR-204] Unsupported Kafka library: confluent")
        return None

    topic = "superstream.metadata_v1"
    consumer = None
    try:
        consumer = builder(bootstrap, cfg)

        from confluent_kafka import TopicPartition  # type: ignore

        # Warm up the connection before attempting list_topics
        logger.debug("Warming up connection with initial poll...")
        try:
            consumer.poll(timeout=2.0)
            logger.debug("Connection warm-up completed")
        except Exception as e:
            logger.debug("Connection warm-up failed, continuing anyway: {}", e)

        # Try to get topic metadata, but don't fail if it doesn't work
        try:
            metadata = consumer.list_topics(topic=topic, timeout=5.0)
            if topic not in metadata.topics:
                logger.error(
                    "[ERR-201] Superstream internal topic is missing. Please ensure permissions for superstream.* topics."
                )
                consumer.close()
                return None
        except Exception as e:
            logger.debug("Could not list topics, proceeding with assignment: {}", e)
            # Continue with topic assignment even if list_topics fails

        # Assign to partition 0
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])
        
        # Poll to ensure assignment is complete and add a small delay
        consumer.poll(timeout=1.0)
        time.sleep(0.1)  # Small delay to ensure consumer is ready
        
        # Get watermark offsets to find the last message
        try:
            low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
        except Exception as e:
            logger.error("[ERR-203] Failed to get watermark offsets: {}", e)
            consumer.close()
            return None
            
        if high == 0:
            logger.error(
                "[ERR-202] Unable to retrieve optimizations data from Superstream – topic empty."
            )
            consumer.close()
            return None
        
        # Seek to the last message (high - 1)
        try:
            consumer.seek(TopicPartition(topic, 0, high - 1))
        except Exception as e:
            logger.error("[ERR-203] Failed to seek to last message: {}", e)
            consumer.close()
            return None
            
        # Poll for the message
        msg = consumer.poll(timeout=5.0)
        consumer.close()
        
        if msg and msg.value():
            return json.loads(msg.value().decode())
        else:
            logger.error("[ERR-202] No message found at last offset")
            return None
            
    except Exception as exc:
        logger.error("[ERR-203] Failed to fetch metadata: {}", exc)
        if consumer:
            try:
                consumer.close()
            except:
                pass
    return None

async def fetch_metadata_aiokafka(
    bootstrap: str,
    cfg: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Fetch metadata using aiokafka library."""
    
    builder = _CONSUMER_BUILDERS.get("aiokafka")
    if builder is None:
        logger.error("[ERR-204] Unsupported Kafka library: aiokafka")
        return None

    topic = "superstream.metadata_v1"
    consumer = None
    try:
        consumer = builder(bootstrap, cfg)
        
        # Start the consumer first
        await consumer.start()

        # For aiokafka, partitions_for_topic returns a set, not a coroutine
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            logger.error(
                "[ERR-201] Superstream internal topic is missing. Please ensure permissions for superstream.* topics."
            )
            await consumer.stop()
            return None

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
            await consumer.stop()
            return None
            
        consumer.seek(tp, end - 1)
        recs = await consumer.getmany(timeout_ms=5000)
        await consumer.stop()
        for batch in recs.values():
            for rec in batch:
                return json.loads(rec.value.decode())
    except Exception as exc:
        logger.error("[ERR-203] Failed to fetch metadata: {}", exc)
        if consumer:
            try:
                await consumer.stop()
            except:
                pass
    return None

def fetch_metadata_sync(
    bootstrap: str,
    cfg: Dict[str, Any],
    lib_name: Literal["kafka-python", "aiokafka", "confluent"],
) -> Optional[Dict[str, Any]]:
    """Synchronous wrapper for fetch_metadata."""
    import asyncio
    
    if lib_name == "kafka-python":
        return fetch_metadata_kafka_python(bootstrap, cfg)
    elif lib_name == "confluent":
        return fetch_metadata_confluent(bootstrap, cfg)
    elif lib_name == "aiokafka":
        # For aiokafka, we need to handle the async case
        try:
            # Try to get the current event loop
            loop = asyncio.get_running_loop()
            # If we get here, there's already a running event loop
            # We need to create a new event loop in a separate thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, fetch_metadata_aiokafka(bootstrap, cfg))
                return future.result()
        except RuntimeError:
            # No event loop is running, we can use asyncio.run()
            return asyncio.run(fetch_metadata_aiokafka(bootstrap, cfg))
    else:
        logger.error("[ERR-204] Unsupported Kafka library: {}", lib_name)
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