"""Core manager for Superstream functionality."""

import json
import threading
import time
import asyncio
from typing import Any, Dict, List, Optional, Set, Tuple

from ..util.logger import get_logger
from ..model.messages import MetadataMessage, TopicConfiguration
from ..util.config import (
    copy_client_configuration_properties,
    convert_to_dot_syntax,
    get_original_config,
)
from ..util.network import get_host_info

logger = get_logger("core.manager")

_DEFAULTS = {"compression.type": "zstd", "batch.size": 16_384, "linger.ms": 5_000}

# Cache for metadata messages per cluster
_METADATA_CACHE: Dict[str, MetadataMessage] = {}
_CACHE_LOCK = threading.RLock()

def normalize_bootstrap(bootstrap: Any) -> str:
    """Normalize bootstrap servers to ensure consistent cache keys.
    
    Args:
        bootstrap: Can be:
            - String: "host1:port1,host2:port2"
            - List/Tuple: ["host1:port1", "host2:port2"]
            - Single string: "host1:port1"
            
    Returns:
        Normalized string with lowercase hosts and sorted order
    """
    if isinstance(bootstrap, (list, tuple)):
        servers = [str(s).strip().lower() for s in bootstrap]
    else:
        servers = [s.strip().lower() for s in str(bootstrap).split(",")]
    return ",".join(sorted(servers))

def get_cached_metadata(bootstrap: str) -> Optional[MetadataMessage]:
    """Get cached metadata for specific cluster."""
    with _CACHE_LOCK:
        normalized_bs = normalize_bootstrap(bootstrap)
        return _METADATA_CACHE.get(normalized_bs)

def update_metadata_cache(bootstrap: str, metadata: MetadataMessage) -> None:
    """Update the metadata cache with new data for specific cluster."""
    with _CACHE_LOCK:
        normalized_bs = normalize_bootstrap(bootstrap)
        _METADATA_CACHE[normalized_bs] = metadata

def fetch_metadata_kafka_python(bootstrap: str, cfg: Dict[str, Any]) -> Optional[MetadataMessage]:
    """Fetch metadata using kafka-python library."""
    from kafka import KafkaConsumer  # type: ignore
    
    topic = "superstream.metadata_v1"
    try:
        # Convert configuration to kafka-python format
        kafka_cfg = {
            "bootstrap_servers": bootstrap,
            "client_id": "superstream-metadata-fetcher",
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "group_id": "superstream-metadata-fetcher",
        }
        copy_client_configuration_properties(cfg, kafka_cfg)
        
        # Create consumer and fetch message
        consumer = KafkaConsumer(topic, **kafka_cfg)
        messages = consumer.poll(timeout_ms=1000)
        consumer.close()
        
        # Process messages
        for tp, msgs in messages.items():
            if not msgs:
                continue
            # Get the latest message
            msg = msgs[-1]
            try:
                metadata = MetadataMessage.from_json(msg.value)
                if metadata:
                    update_metadata_cache(bootstrap, metadata)
                    return metadata
            except Exception as e:
                logger.error("[ERR-102] Failed to parse metadata message: {}", e)
                continue
                
        logger.warn("[WARN-101] No metadata message found in topic {}", topic)
        return None
    except Exception as e:
        logger.error("[ERR-103] Failed to fetch metadata: {}", e)
        return None

def fetch_metadata_confluent(bootstrap: str, cfg: Dict[str, Any]) -> Optional[MetadataMessage]:
    """Fetch metadata using confluent-kafka library."""
    from confluent_kafka import Consumer  # type: ignore
    
    topic = "superstream.metadata_v1"
    try:
        # Convert configuration to confluent format
        kafka_cfg = {
            "bootstrap.servers": bootstrap,
            "client.id": "superstream-metadata-fetcher",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "group.id": "superstream-metadata-fetcher",
        }
        copy_client_configuration_properties(cfg, kafka_cfg)
        
        # Create consumer and fetch message
        consumer = Consumer(kafka_cfg)
        consumer.subscribe([topic])
        messages = consumer.poll(timeout=1.0)
        consumer.close()
        
        if messages:
            try:
                metadata = MetadataMessage.from_json(messages.value())
                if metadata:
                    update_metadata_cache(bootstrap, metadata)
                    return metadata
            except Exception as e:
                logger.error("[ERR-102] Failed to parse metadata message: {}", e)
                
        logger.warn("[WARN-101] No metadata message found in topic {}", topic)
        return None
    except Exception as e:
        logger.error("[ERR-103] Failed to fetch metadata: {}", e)
        return None

async def fetch_metadata_aiokafka(bootstrap: str, cfg: Dict[str, Any]) -> Optional[MetadataMessage]:
    """Fetch metadata using aiokafka library."""
    from aiokafka import AIOKafkaConsumer  # type: ignore
    
    topic = "superstream.metadata_v1"
    try:
        # Convert configuration to aiokafka format
        kafka_cfg = {
            "bootstrap_servers": bootstrap,
            "client_id": "superstream-metadata-fetcher",
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "group_id": "superstream-metadata-fetcher",
        }
        copy_client_configuration_properties(cfg, kafka_cfg)
        
        # Create consumer and fetch message
        consumer = AIOKafkaConsumer(topic, **kafka_cfg)
        await consumer.start()
        try:
            messages = await consumer.getmany(timeout_ms=1000)
            if messages:
                for tp, msgs in messages.items():
                    if msgs:
                        msg = msgs[-1]
                        try:
                            metadata = MetadataMessage.from_json(msg.value)
                            if metadata:
                                update_metadata_cache(bootstrap, metadata)
                                return metadata
                        except Exception as e:
                            logger.error("[ERR-102] Failed to parse metadata message: {}", e)
                            continue
        finally:
            await consumer.stop()
            
        logger.warn("[WARN-101] No metadata message found in topic {}", topic)
        return None
    except Exception as e:
        logger.error("[ERR-103] Failed to fetch metadata: {}", e)
        return None

def fetch_metadata(bootstrap: str, cfg: Dict[str, Any]) -> Optional[MetadataMessage]:
    """Fetch the latest optimization metadata from superstream.metadata_v1."""
    # First check cache for this specific cluster
    cached_metadata = get_cached_metadata(bootstrap)
    if cached_metadata is not None:
        return cached_metadata

    # Determine library from configuration
    lib = cfg.get("lib", "kafka-python")
    
    if lib == "kafka-python":
        return fetch_metadata_kafka_python(bootstrap, cfg)
    elif lib == "confluent":
        return fetch_metadata_confluent(bootstrap, cfg)
    elif lib == "aiokafka":
        return asyncio.run(fetch_metadata_aiokafka(bootstrap, cfg))
    else:
        logger.error("[ERR-104] Unsupported Kafka library: {}", lib)
        return None

def optimal_cfg(metadata: Optional[MetadataMessage], topics: list[str], orig: Dict[str, Any]) -> Dict[str, Any]:
    """Compute optimal configuration based on metadata and topics."""
    from ..util.config import is_latency_sensitive
    latency = is_latency_sensitive()
    cfg: Dict[str, Any]
    if not metadata or not metadata.topics_configuration:
        cfg = dict(_DEFAULTS)
    else:
        matches = [tc for tc in metadata.topics_configuration if tc.topic_name in topics]
        if not matches:
            cfg = dict(_DEFAULTS)
        else:
            best = max(matches, key=lambda tc: tc.potential_reduction_percentage * tc.daily_writes_bytes)
            cfg = dict(best.optimized_configuration)
            for k, v in _DEFAULTS.items():
                cfg.setdefault(k, v)
    if latency:
        cfg.pop("linger.ms", None)
    for p in ("batch.size", "linger.ms"):
        if p in orig and p in cfg:
            try:
                if int(orig[p]) > int(cfg[p]):
                    cfg[p] = orig[p]
            except Exception:
                pass
    return cfg 