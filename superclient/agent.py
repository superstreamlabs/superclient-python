from __future__ import annotations

"""Superstream Python agent (superclient) – see README for details."""

import builtins
import json
import os
import sys
import threading
import time
import uuid
from typing import Any, Dict, Optional

from .logger import get_logger, set_debug_enabled

# ---------------------------------------------------------------------------
# Environment & constants
# ---------------------------------------------------------------------------

_ENV_VARS: Dict[str, str] = {k: v for k, v in os.environ.items() if k.startswith("SUPERSTREAM_")}
_DISABLED = os.getenv("SUPERSTREAM_DISABLED", "false").lower() == "true"
if os.getenv("SUPERSTREAM_DEBUG", "false").lower() == "true":
    set_debug_enabled(True)

logger = get_logger("agent")
logger.info("Superstream Agent initialized with environment variables: {}", _ENV_VARS)
if _DISABLED:
    logger.warn("Superstream functionality disabled via SUPERSTREAM_DISABLED")

_SUPERLIB_PREFIX = "superstreamlib-"
_DEFAULTS = {"compression.type": "zstd", "batch.size": 16_384, "linger.ms": 5_000}
_DEFAULT_REPORT_INTERVAL_MS = 300_000

_PRODUCER_TRACKERS: Dict[str, "_ProducerTracker"] = {}
_TRACK_LOCK = threading.RLock()
_PATCHED: Dict[str, bool] = {}
_original_import = builtins.__import__

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _mask_sensitive(k: str, v: Any) -> Any:
    return "[MASKED]" if "password" in k.lower() or "sasl.jaas.config" in k.lower() else v


def _copy_security(src: Dict[str, Any], dst: Dict[str, Any]):
    keys = [
        "security.protocol",
        "sasl.mechanism",
        "sasl.jaas.config",
        "ssl.keystore.password",
        "ssl.truststore.password",
        "ssl.key.password",
        "client.dns.lookup",
    ]
    for k in keys:
        if k in src and k not in dst:
            dst[k] = src[k]


def _internal_send_clients(bootstrap: str, base_cfg: Dict[str, Any], payload: bytes) -> None:
    """Send *payload* to superstream.clients using whatever Kafka lib is available.

    Tries kafka-python, then confluent-kafka. Silent failure if both unavailable.
    """
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
        _copy_security(base_cfg, cfg)
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
        _copy_security(base_cfg, cfg)
        prod = _CProducer(cfg)
        prod.produce("superstream.clients", payload)
        prod.flush()
    except Exception:
        # As a last resort just log and drop – should never interrupt app
        logger.debug("Failed to send clients message via all libraries")


# ---------------------------------------------------------------------------
# Metadata fetch (kafka-python only for now)
# ---------------------------------------------------------------------------

def _fetch_metadata(bootstrap: str, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    topic = "superstream.metadata_v1"
    try:
        import kafka  # type: ignore

        consumer_cfg = {
            "bootstrap_servers": bootstrap,
            "client_id": _SUPERLIB_PREFIX + "metadata-consumer",
            "group_id": None,
            "enable_auto_commit": False,
            "auto_offset_reset": "latest",
        }
        _copy_security(cfg, consumer_cfg)
        c = kafka.KafkaConsumer(**consumer_cfg)
        if not c.partitions_for_topic(topic):
            logger.error(
                "Superstream internal topic is missing. Please ensure permissions for superstream.* topics."
            )
            c.close()
            return None
        tp = kafka.TopicPartition(topic, 0)
        c.assign([tp])
        c.seek_to_end(tp)
        end = c.position(tp)
        if end == 0:
            logger.error(
                "Unable to retrieve optimizations data from Superstream – topic empty."
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
        logger.error("Failed to fetch metadata: {}", exc)
    return None


# ---------------------------------------------------------------------------
# Optimisation logic
# ---------------------------------------------------------------------------

def _optimal_cfg(metadata: Optional[Dict[str, Any]], topics: list[str], orig: Dict[str, Any]) -> Dict[str, Any]:
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


# ---------------------------------------------------------------------------
# Producer tracker & heartbeat
# ---------------------------------------------------------------------------

class _ProducerTracker:
    def __init__(
        self,
        lib: str,
        producer: Any,
        bootstrap: str,
        client_id: str,
        orig_cfg: Dict[str, Any],
        opt_cfg: Dict[str, Any],
        report_interval_ms: int,
    ) -> None:
        self.uuid = str(uuid.uuid4())
        self.library = lib
        self.producer = producer
        self.bootstrap = bootstrap
        self.client_id = client_id
        self.orig_cfg = orig_cfg
        self.opt_cfg = opt_cfg
        self.topics: set[str] = set()
        self.report_interval_ms = report_interval_ms or _DEFAULT_REPORT_INTERVAL_MS
        self.last_hb = 0.0
        self.active = True

    def record_topic(self, topic: str):
        self.topics.add(topic)

    def close(self):
        self.active = False


def _determine_topic(tr: _ProducerTracker) -> str:
    return sorted(tr.topics)[0] if tr.topics else ""


class _Heartbeat(threading.Thread):
    _singleton: Optional["_Heartbeat"] = None
    _lock = threading.Lock()

    def __init__(self):
        super().__init__(name="superstream-heartbeat", daemon=True)
        self._stop = threading.Event()

    @classmethod
    def ensure(cls):
        with cls._lock:
            if cls._singleton is None or not cls._singleton.is_alive():
                cls._singleton = _Heartbeat()
                cls._singleton.start()

    def run(self):
        while not self._stop.is_set():
            now = time.time()
            with _TRACK_LOCK:
                trackers = list(_PRODUCER_TRACKERS.values())
            for tr in trackers:
                if not tr.active:
                    continue
                if (now - tr.last_hb) * 1000 < tr.report_interval_ms:
                    continue
                _send_clients_msg(tr)
                tr.last_hb = now
            time.sleep(1)


# ---------------------------------------------------------------------------
# Clients topic producer (kafka-python only)
# ---------------------------------------------------------------------------

def _send_clients_msg(tr: _ProducerTracker, error: str = "") -> None:
    msg_dict = {
        "client_id": tr.client_id,
        "ip_address": _get_ip(),
        "type": "producer",
        "message_type": "client_stats" if not error else "client_info",
        "version": _VERSION,
        "topics": sorted(tr.topics),
        "original_configuration": {k: _mask_sensitive(k, v) for k, v in tr.orig_cfg.items()},
        "optimized_configuration": {k: _mask_sensitive(k, v) for k, v in tr.opt_cfg.items()},
        "environment_variables": _ENV_VARS,
        "hostname": _get_hostname(),
        "superstream_client_uid": tr.uuid,
        "most_impactful_topic": _determine_topic(tr),
        "language": "Python",
        "error": error,
    }
    payload = json.dumps(msg_dict).encode()
    _internal_send_clients(tr.bootstrap, tr.orig_cfg, payload)
    logger.debug("Sent clients message for {}", tr.client_id)


# ---------------------------------------------------------------------------
# kafka-python patching
# ---------------------------------------------------------------------------

def _patch_kafka_python(mod):
    if _PATCHED.get("kafka-python"):
        return
    _PATCHED["kafka-python"] = True
    Producer = mod.KafkaProducer
    orig_init = Producer.__init__

    def init_patch(self, *args, **kwargs):  # noqa: D401
        if _DISABLED:
            return orig_init(self, *args, **kwargs)
        orig_cfg = dict(kwargs)
        bootstrap = orig_cfg.get("bootstrap_servers") or (args[0] if args else None)
        if not bootstrap:
            return orig_init(self, *args, **kwargs)
        client_id = orig_cfg.get("client_id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, *args, **kwargs)
        topics_env = [t.strip() for t in os.getenv("SUPERSTREAM_TOPICS_LIST", "").split(",") if t.strip()]
        metadata = _fetch_metadata(bootstrap, orig_cfg)
        opt_cfg = _optimal_cfg(metadata, topics_env, orig_cfg)
        for k, v in opt_cfg.items():
            snake = k.replace(".", "_")
            if kwargs.get(snake) != v:
                logger.debug("Overriding configuration: {} -> {}", snake, v)
                kwargs[snake] = v
        report_interval = metadata.get("report_interval_ms") if metadata else _DEFAULT_REPORT_INTERVAL_MS
        tr = _ProducerTracker(
            lib="kafka-python",
            producer=self,
            bootstrap=bootstrap,
            client_id=client_id,
            orig_cfg=orig_cfg,
            opt_cfg=opt_cfg,
            report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
        )
        with _TRACK_LOCK:
            _PRODUCER_TRACKERS[tr.uuid] = tr
        orig_init(self, *args, **kwargs)
        if not hasattr(self, "_superstream_patch"):
            original_send = self.send

            def send_patch(inner, topic, *a, **kw):
                tr.record_topic(topic)
                return original_send(topic, *a, **kw)

            self.send = send_patch  # type: ignore
            orig_close = self.close

            def close_patch(inner, *a, **kw):
                tr.close()
                with _TRACK_LOCK:
                    _PRODUCER_TRACKERS.pop(tr.uuid, None)
                return orig_close(*a, **kw)

            self.close = close_patch  # type: ignore
            self._superstream_patch = True  # type: ignore
        _send_clients_msg(tr)
        logger.info("Successfully optimized producer configuration for {}", client_id)

    Producer.__init__ = init_patch  # type: ignore


# ---------------------------------------------------------------------------
# aiokafka patching (async)
# ---------------------------------------------------------------------------

def _patch_aiokafka(mod):
    if _PATCHED.get("aiokafka"):
        return
    _PATCHED["aiokafka"] = True

    Producer = mod.AIOKafkaProducer
    orig_init = Producer.__init__

    def init_patch(self, *args, **kwargs):  # noqa: D401
        if _DISABLED:
            return orig_init(self, *args, **kwargs)
        orig_cfg = dict(kwargs)
        bootstrap = orig_cfg.get("bootstrap_servers")
        if not bootstrap and args:
            bootstrap = args[0]
        if not bootstrap:
            return orig_init(self, *args, **kwargs)
        client_id = orig_cfg.get("client_id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, *args, **kwargs)
        topics_env = [t.strip() for t in os.getenv("SUPERSTREAM_TOPICS_LIST", "").split(",") if t.strip()]
        metadata = _fetch_metadata(bootstrap, orig_cfg)
        opt_cfg = _optimal_cfg(metadata, topics_env, orig_cfg)
        for k, v in opt_cfg.items():
            if kwargs.get(k) != v:
                logger.debug("Overriding configuration: {} -> {}", k, v)
                kwargs[k] = v
        report_interval = metadata.get("report_interval_ms") if metadata else _DEFAULT_REPORT_INTERVAL_MS
        tr = _ProducerTracker(
            lib="aiokafka",
            producer=self,
            bootstrap=bootstrap,
            client_id=client_id,
            orig_cfg=orig_cfg,
            opt_cfg=opt_cfg,
            report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
        )
        with _TRACK_LOCK:
            _PRODUCER_TRACKERS[tr.uuid] = tr
        orig_init(self, *args, **kwargs)

        if not hasattr(self, "_superstream_patch"):
            original_send = self.send

            async def send_patch(inner, topic, *a, **kw):  # type: ignore
                tr.record_topic(topic)
                return await original_send(topic, *a, **kw)

            self.send = send_patch  # type: ignore

            original_stop = self.stop

            async def stop_patch(inner, *a, **kw):  # type: ignore
                await original_stop(*a, **kw)
                tr.close()
                with _TRACK_LOCK:
                    _PRODUCER_TRACKERS.pop(tr.uuid, None)

            self.stop = stop_patch  # type: ignore
            self._superstream_patch = True  # type: ignore
        _send_clients_msg(tr)
        logger.info("Successfully optimized producer configuration for {}", client_id)

    Producer.__init__ = init_patch  # type: ignore


# ---------------------------------------------------------------------------
# confluent-kafka patching
# ---------------------------------------------------------------------------

def _patch_confluent(mod):
    if _PATCHED.get("confluent"):
        return
    _PATCHED["confluent"] = True

    Producer = mod.Producer
    orig_init = Producer.__init__

    def init_patch(self, conf: Dict[str, Any], *args, **kwargs):  # noqa: D401
        if _DISABLED:
            return orig_init(self, conf, *args, **kwargs)
        conf = dict(conf)
        bootstrap = conf.get("bootstrap.servers")
        client_id = conf.get("client.id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, conf, *args, **kwargs)
        topics_env = [t.strip() for t in os.getenv("SUPERSTREAM_TOPICS_LIST", "").split(",") if t.strip()]
        metadata = _fetch_metadata(bootstrap, conf)
        opt_cfg = _optimal_cfg(metadata, topics_env, conf)
        for k, v in opt_cfg.items():
            if conf.get(k) != v:
                logger.debug("Overriding configuration: {} -> {}", k, v)
                conf[k] = v
        report_interval = metadata.get("report_interval_ms") if metadata else _DEFAULT_REPORT_INTERVAL_MS
        tr = _ProducerTracker(
            lib="confluent",
            producer=self,
            bootstrap=bootstrap,
            client_id=client_id,
            orig_cfg=conf,
            opt_cfg=opt_cfg,
            report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
        )
        with _TRACK_LOCK:
            _PRODUCER_TRACKERS[tr.uuid] = tr
        orig_init(self, conf, *args, **kwargs)

        if not hasattr(self, "_superstream_patch"):
            original_produce = self.produce

            def produce_patch(inner, topic, *a, **kw):  # type: ignore
                tr.record_topic(topic)
                return original_produce(topic, *a, **kw)

            self.produce = produce_patch  # type: ignore

            # Flush or close to mark shutdown
            orig_flush = self.flush

            def flush_patch(inner, *a, **kw):  # type: ignore
                tr.close()
                with _TRACK_LOCK:
                    _PRODUCER_TRACKERS.pop(tr.uuid, None)
                return orig_flush(*a, **kw)

            self.flush = flush_patch  # type: ignore
            if hasattr(self, "close"):
                orig_close = self.close  # type: ignore

                def close_patch(inner, *a, **kw):  # type: ignore
                    tr.close()
                    with _TRACK_LOCK:
                        _PRODUCER_TRACKERS.pop(tr.uuid, None)
                    return orig_close(*a, **kw)

                self.close = close_patch  # type: ignore
            self._superstream_patch = True  # type: ignore
        _send_clients_msg(tr)
        logger.info("Successfully optimized producer configuration for {}", client_id)

    Producer.__init__ = init_patch  # type: ignore


# ---------------------------------------------------------------------------
# Import hook
# ---------------------------------------------------------------------------

def _import_hook(name, globals=None, locals=None, fromlist=(), level=0):
    module = _original_import(name, globals, locals, fromlist, level)
    try:
        if name.startswith("kafka") and not _PATCHED.get("kafka-python"):
            import kafka as kafka_mod  # type: ignore
            _patch_kafka_python(kafka_mod)
        elif name.startswith("aiokafka") and not _PATCHED.get("aiokafka"):
            import aiokafka as aio_mod  # type: ignore
            _patch_aiokafka(aio_mod)
        elif name.startswith("confluent_kafka") and not _PATCHED.get("confluent"):
            import confluent_kafka as conf_mod  # type: ignore
            _patch_confluent(conf_mod)
    except Exception as exc:
        logger.error("Failed to patch library {}: {}", name, exc)
    return module


# ---------------------------------------------------------------------------
# Host info helpers & version
# ---------------------------------------------------------------------------

_HOSTNAME: Optional[str] = None
_IP: Optional[str] = None


def _get_hostname() -> str:
    global _HOSTNAME
    if _HOSTNAME is None:
        try:
            import socket

            _HOSTNAME = socket.gethostname()
        except Exception:
            _HOSTNAME = ""
    return _HOSTNAME


def _get_ip() -> str:
    global _IP
    if _IP is None:
        try:
            import socket

            _IP = socket.gethostbyname(socket.gethostname())
        except Exception:
            _IP = ""
    return _IP

_VERSION = "0.1.0"

# ---------------------------------------------------------------------------
# Initialise
# ---------------------------------------------------------------------------

def initialize():
    if _DISABLED:
        return
    if builtins.__import__ is not _import_hook:
        builtins.__import__ = _import_hook  # type: ignore
    if "kafka" in sys.modules and not _PATCHED.get("kafka-python"):
        try:
            _patch_kafka_python(sys.modules["kafka"])
        except Exception as exc:
            logger.error("Failed to patch pre-imported kafka module: {}", exc)
    _Heartbeat.ensure() 