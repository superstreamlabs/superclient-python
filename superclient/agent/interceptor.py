"""Producer interception functionality."""

import os
from typing import Any, Dict

from ..logger import get_logger
from ..util.config import get_topics_list, is_disabled
from ..core.manager import fetch_metadata, optimal_cfg
from ..core.reporter import send_clients_msg
from .tracker import ProducerTracker, Heartbeat

logger = get_logger("agent.interceptor")

_PATCHED: Dict[str, bool] = {}
_SUPERLIB_PREFIX = "superstreamlib-"
_DEFAULT_REPORT_INTERVAL_MS = 300_000

def patch_kafka_python(mod):
    """Patch kafka-python producer."""
    if _PATCHED.get("kafka-python"):
        return
    _PATCHED["kafka-python"] = True
    Producer = mod.KafkaProducer
    orig_init = Producer.__init__

    def init_patch(self, *args, **kwargs):
        if is_disabled():
            return orig_init(self, *args, **kwargs)
        orig_cfg = dict(kwargs)
        bootstrap = orig_cfg.get("bootstrap_servers") or (args[0] if args else None)
        if not bootstrap:
            return orig_init(self, *args, **kwargs)
        client_id = orig_cfg.get("client_id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, *args, **kwargs)
        topics_env = get_topics_list()
        metadata = fetch_metadata(bootstrap, orig_cfg)
        opt_cfg = optimal_cfg(metadata, topics_env, orig_cfg)
        for k, v in opt_cfg.items():
            snake = k.replace(".", "_")
            if kwargs.get(snake) != v:
                logger.debug("Overriding configuration: {} -> {}", snake, v)
                kwargs[snake] = v
        report_interval = metadata.report_interval_ms if metadata else _DEFAULT_REPORT_INTERVAL_MS
        tr = ProducerTracker(
            lib="kafka-python",
            producer=self,
            bootstrap=bootstrap,
            client_id=client_id,
            orig_cfg=orig_cfg,
            opt_cfg=opt_cfg,
            report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
        )
        Heartbeat.register_tracker(tr)
        orig_init(self, *args, **kwargs)
        if not hasattr(self, "_superstream_patch"):
            original_send = self.send

            def send_patch(inner, topic, *a, **kw):
                tr.record_topic(topic)
                return original_send(topic, *a, **kw)

            self.send = send_patch
            orig_close = self.close

            def close_patch(inner, *a, **kw):
                tr.close()
                Heartbeat.unregister_tracker(tr.uuid)
                return orig_close(*a, **kw)

            self.close = close_patch
            self._superstream_patch = True
        send_clients_msg(tr)
        logger.info("Successfully optimized producer configuration for {}", client_id)

    Producer.__init__ = init_patch

def patch_aiokafka(mod):
    """Patch aiokafka producer."""
    if _PATCHED.get("aiokafka"):
        return
    _PATCHED["aiokafka"] = True
    Producer = mod.AIOKafkaProducer
    orig_init = Producer.__init__

    def init_patch(self, *args, **kwargs):
        if is_disabled():
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
        topics_env = get_topics_list()
        metadata = fetch_metadata(bootstrap, orig_cfg)
        opt_cfg = optimal_cfg(metadata, topics_env, orig_cfg)
        for k, v in opt_cfg.items():
            if kwargs.get(k) != v:
                logger.debug("Overriding configuration: {} -> {}", k, v)
                kwargs[k] = v
        report_interval = metadata.report_interval_ms if metadata else _DEFAULT_REPORT_INTERVAL_MS
        tr = ProducerTracker(
            lib="aiokafka",
            producer=self,
            bootstrap=bootstrap,
            client_id=client_id,
            orig_cfg=orig_cfg,
            opt_cfg=opt_cfg,
            report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
        )
        Heartbeat.register_tracker(tr)
        orig_init(self, *args, **kwargs)
        if not hasattr(self, "_superstream_patch"):
            original_send = self.send

            async def send_patch(inner, topic, *a, **kw):
                tr.record_topic(topic)
                return await original_send(topic, *a, **kw)

            self.send = send_patch
            original_stop = self.stop

            async def stop_patch(inner, *a, **kw):
                await original_stop(*a, **kw)
                tr.close()
                Heartbeat.unregister_tracker(tr.uuid)

            self.stop = stop_patch
            self._superstream_patch = True
        send_clients_msg(tr)
        logger.info("Successfully optimized producer configuration for {}", client_id)

    Producer.__init__ = init_patch

def patch_confluent(mod):
    """Patch confluent-kafka producer."""
    if _PATCHED.get("confluent"):
        return
    _PATCHED["confluent"] = True
    Producer = mod.Producer
    orig_init = Producer.__init__

    def init_patch(self, conf: Dict[str, Any], *args, **kwargs):
        if is_disabled():
            return orig_init(self, conf, *args, **kwargs)
        conf = dict(conf)
        bootstrap = conf.get("bootstrap.servers")
        client_id = conf.get("client.id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, conf, *args, **kwargs)
        topics_env = get_topics_list()
        metadata = fetch_metadata(bootstrap, conf)
        opt_cfg = optimal_cfg(metadata, topics_env, conf)
        for k, v in opt_cfg.items():
            if conf.get(k) != v:
                logger.debug("Overriding configuration: {} -> {}", k, v)
                conf[k] = v
        report_interval = metadata.report_interval_ms if metadata else _DEFAULT_REPORT_INTERVAL_MS
        tr = ProducerTracker(
            lib="confluent",
            producer=self,
            bootstrap=bootstrap,
            client_id=client_id,
            orig_cfg=conf,
            opt_cfg=opt_cfg,
            report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
        )
        Heartbeat.register_tracker(tr)
        orig_init(self, conf, *args, **kwargs)
        if not hasattr(self, "_superstream_patch"):
            original_produce = self.produce

            def produce_patch(inner, topic, *a, **kw):
                tr.record_topic(topic)
                return original_produce(topic, *a, **kw)

            self.produce = produce_patch
            orig_flush = self.flush

            def flush_patch(inner, *a, **kw):
                tr.close()
                Heartbeat.unregister_tracker(tr.uuid)
                return orig_flush(*a, **kw)

            self.flush = flush_patch
            if hasattr(self, "close"):
                orig_close = self.close

                def close_patch(inner, *a, **kw):
                    tr.close()
                    Heartbeat.unregister_tracker(tr.uuid)
                    return orig_close(*a, **kw)

                self.close = close_patch
            self._superstream_patch = True
        send_clients_msg(tr)
        logger.info("Successfully optimized producer configuration for {}", client_id)

    Producer.__init__ = init_patch 