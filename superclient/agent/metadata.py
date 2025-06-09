"""Metadata handling functionality."""

import json
import os
from typing import Any, Dict, Optional

from ..logger import get_logger

logger = get_logger("agent.metadata")

_DEFAULTS = {"compression.type": "zstd", "batch.size": 16_384, "linger.ms": 5_000}

def fetch_metadata(bootstrap: str, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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
        from .clients import copy_security
        copy_security(cfg, consumer_cfg)
        c = kafka.KafkaConsumer(**consumer_cfg)
        if not c.partitions_for_topic(topic):
            logger.error(
                "[ERR-201] Superstream internal topic is missing. Please ensure permissions for superstream.* topics."
            )
            c.close()
            return None
        tp = kafka.TopicPartition(topic, 0)
        c.assign([tp])
        c.seek_to_end(tp)
        end = c.position(tp)
        if end == 0:
            logger.error(
                "[ERR-202] Unable to retrieve optimizations data from Superstream – topic empty."
            )
            c.close()
            return None
        c.seek(tp, end - 1)
        recs = c.poll(timeout_ms=5000)
        c.close()
        for batch in recs.values():
            for rec in batch:
                return json.loads(rec.value.decode())
    except Exception as exc:
        logger.error("[ERR-203] Failed to fetch metadata: {}", exc)
    return None

def optimal_cfg(metadata: Optional[Dict[str, Any]], topics: list[str], orig: Dict[str, Any]) -> Dict[str, Any]:
    """Compute optimal configuration based on metadata and topics."""
    latency = os.getenv("SUPERSTREAM_LATENCY_SENSITIVE", "false").lower() == "true"
    cfg: Dict[str, Any]
    if not metadata or not metadata.get("topics_configuration"):
        cfg = dict(_DEFAULTS)
    else:
        matches = [tc for tc in metadata["topics_configuration"] if tc["topic_name"] in topics]
        if not matches:
            cfg = dict(_DEFAULTS)
        else:
            best = max(matches, key=lambda tc: tc["potential_reduction_percentage"] * tc["daily_writes_bytes"])
            cfg = dict(best.get("optimized_configuration", {}))
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