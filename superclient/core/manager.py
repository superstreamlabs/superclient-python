"""Core manager for Superstream functionality."""

import json
from typing import Any, Dict, Optional

from ..logger import get_logger
from ..model.messages import MetadataMessage, TopicConfiguration
from ..util.config import copy_security

logger = get_logger("core.manager")

_DEFAULTS = {"compression.type": "zstd", "batch.size": 16_384, "linger.ms": 5_000}

def fetch_metadata(bootstrap: str, cfg: Dict[str, Any]) -> Optional[MetadataMessage]:
    """Fetch the latest optimization metadata from superstream.metadata_v1."""
    topic = "superstream.metadata_v1"
    try:
        import kafka  # type: ignore

        consumer_cfg = {
            "bootstrap_servers": bootstrap,
            "client_id": "superstreamlib-metadata-consumer",
            "group_id": None,
            "enable_auto_commit": False,
            "auto_offset_reset": "latest",
        }
        copy_security(cfg, consumer_cfg)
        c = kafka.KafkaConsumer(**consumer_cfg)
        if not c.partitions_for_topic(topic):
            logger.error(
                "[ERR-101] Superstream internal topic is missing. Please ensure permissions for superstream.* topics."
            )
            c.close()
            return None
        tp = kafka.TopicPartition(topic, 0)
        c.assign([tp])
        c.seek_to_end(tp)
        end = c.position(tp)
        if end == 0:
            logger.error(
                "[ERR-102] Unable to retrieve optimizations data from Superstream – topic empty."
            )
            c.close()
            return None
        c.seek(tp, end - 1)
        recs = c.poll(timeout_ms=5000)
        c.close()
        for batch in recs.values():
            for rec in batch:
                data = json.loads(rec.value.decode())
                return MetadataMessage(
                    topics_configuration=[
                        TopicConfiguration(**tc) for tc in data.get("topics_configuration", [])
                    ],
                    report_interval_ms=data.get("report_interval_ms"),
                )
    except Exception as exc:
        logger.error("[ERR-103] Failed to fetch metadata: {}", exc)
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