"""Producer interception functionality."""

import os
from typing import Any, Dict

from ..util.logger import get_logger
from ..util.config import get_topics_list, is_disabled
from .metadata import fetch_metadata, optimal_cfg, _DEFAULTS
from ..core.reporter import send_clients_msg
from ..core.manager import normalize_bootstrap
from .tracker import ProducerTracker, Heartbeat

logger = get_logger("agent.interceptor")

_PATCHED: Dict[str, bool] = {}
_SUPERLIB_PREFIX = "superstreamlib-"
_DEFAULT_REPORT_INTERVAL_MS = 300_000

def patch_kafka_python(mod):
    """Patch kafka-python producer to add Superstream monitoring and optimization."""
    # Skip if already patched
    if _PATCHED.get("kafka-python"):
        return
        
    _PATCHED["kafka-python"] = True
    Producer = mod.KafkaProducer
    orig_init = Producer.__init__

    def init_patch(self, *args, **kwargs):
        """Patched initialization function for KafkaProducer.
        Adds Superstream monitoring and optimizes configuration.
        """
        # If Superstream is disabled, use original initialization
        if is_disabled():
            return orig_init(self, *args, **kwargs)

        # Store original configuration
        orig_cfg = dict(kwargs)
        
        # Get bootstrap servers from args or kwargs
        bootstrap = orig_cfg.get("bootstrap_servers") or (args[0] if args else None)
        if not bootstrap:
            return orig_init(self, *args, **kwargs)
            
        # Normalize bootstrap servers format
        bootstrap = normalize_bootstrap(bootstrap)
        
        # Skip if client is an internal client
        client_id = orig_cfg.get("client_id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, *args, **kwargs)

        try:
            # Get topics and metadata for optimization
            topics_env = get_topics_list()
            metadata = fetch_metadata(bootstrap, orig_cfg, "kafka-python")
            
            # Check if Superstream is active for this cluster
            error_msg = ""
            if metadata is None:
                # not logging as it is already logged in fetch_metadata
                error_msg = "[ERR-304] Failed to fetch metadata for producer with client id {}: Unable to connect to Superstream service".format(client_id)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            elif not metadata.active:
                error_msg = "[ERR-301] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it."
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            else:
                # Get optimized configuration if Superstream is active
                opt_cfg = optimal_cfg(metadata, topics_env, orig_cfg, "kafka-python")
            
            # Apply optimized configuration
            for k, v in opt_cfg.items():
                snake = k.replace(".", "_")
                if kwargs.get(snake) != v:
                    logger.debug("Overriding configuration: {} -> {}", snake, v)
                    kwargs[snake] = v

            # Set up reporting interval
            report_interval = metadata.report_interval_ms if metadata else _DEFAULT_REPORT_INTERVAL_MS
            
            # Create and register producer tracker
            tr = ProducerTracker(
                lib="kafka-python",
                producer=self,
                bootstrap=bootstrap,
                client_id=client_id,
                orig_cfg=orig_cfg,
                opt_cfg=opt_cfg,
                report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
                error=error_msg,  # Store error message in tracker
                metadata=metadata,
                topics_env=topics_env,
            )
            Heartbeat.register_tracker(tr)

            # Initialize the producer with original configuration
            orig_init(self, *args, **kwargs)

            # Patch send and close methods if not already patched
            if not hasattr(self, "_superstream_patch"):
                original_send = self.send

                def send_patch(topic, *a, **kw):
                    """Track topic usage when sending messages."""
                    tr.record_topic(topic)
                    return original_send(topic, *a, **kw)

                self.send = send_patch
                orig_close = self.close

                def close_patch(*a, **kw):
                    """Clean up Superstream resources when closing the producer."""
                    if not hasattr(self, "_superstream_closed"):
                        self._superstream_closed = True
                        tr.close()
                        Heartbeat.unregister_tracker(tr.uuid)
                    return orig_close(*a, **kw)

                self.close = close_patch
                self._superstream_patch = True

            # Initialize again with optimized configuration
            orig_init(self, *args, **kwargs)
            
            # Send client registration message
            send_clients_msg(tr, error_msg)
            
            # Log success message based on whether defaults were used
            if not opt_cfg:  # No optimization applied
                pass  # Skip success message as there was an error
            elif all(opt_cfg.get(k) == v for k, v in _DEFAULTS.items()) and len(opt_cfg) == len(_DEFAULTS):  # Default optimization
                if client_id:
                    logger.info("Successfully optimized producer with default optimization parameters for {}", client_id)
                else:
                    logger.info("Successfully optimized producer with default optimization parameters")
            else:  # Custom optimization
                if client_id:
                    logger.info("Successfully optimized producer configuration for {}", client_id)
                else:
                    logger.info("Successfully optimized producer configuration")

        except Exception as e:
            # If any error occurs in our logic, log it and create the producer normally
            logger.error("[ERR-303] Failed to optimize producer with client id {}: {}", client_id, str(e))
            return orig_init(self, *args, **kwargs)

    # Replace the original initialization with our patched version
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
        bootstrap = normalize_bootstrap(bootstrap)
        client_id = orig_cfg.get("client_id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, *args, **kwargs)

        try:
            topics_env = get_topics_list()
            metadata = fetch_metadata(bootstrap, orig_cfg, "aiokafka")
            error_msg = ""
            if metadata is None:
                error_msg = "[ERR-304] Failed to fetch metadata for producer with client id {}: Unable to connect to Superstream service".format(client_id)
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            elif not metadata.active:
                error_msg = "[ERR-301] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it."
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            else:
                # Get optimized configuration if Superstream is active
                opt_cfg = optimal_cfg(metadata, topics_env, orig_cfg, "aiokafka")
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
                error=error_msg,  # Store error message in tracker
                metadata=metadata,
                topics_env=topics_env,
            )
            Heartbeat.register_tracker(tr)
            orig_init(self, *args, **kwargs)
            if not hasattr(self, "_superstream_patch"):
                original_send = self.send

                async def send_patch(topic, *a, **kw):
                    tr.record_topic(topic)
                    return await original_send(topic, *a, **kw)

                self.send = send_patch
                original_stop = self.stop

                async def stop_patch(*a, **kw):
                    if not hasattr(self, "_superstream_closed"):
                        self._superstream_closed = True
                        tr.close()
                        Heartbeat.unregister_tracker(tr.uuid)
                    await original_stop(*a, **kw)

                self.stop = stop_patch
                self._superstream_patch = True
            orig_init(self, *args, **kwargs)
            send_clients_msg(tr, error_msg)
            
            # Log success message based on whether defaults were used
            if not opt_cfg:  # No optimization applied
                pass  # Skip success message as there was an error
            elif all(opt_cfg.get(k) == v for k, v in _DEFAULTS.items()) and len(opt_cfg) == len(_DEFAULTS):  # Default optimization
                if client_id:
                    logger.info("Successfully optimized producer with default optimization parameters for {}", client_id)
                else:
                    logger.info("Successfully optimized producer with default optimization parameters")
            else:  # Custom optimization
                if client_id:
                    logger.info("Successfully optimized producer configuration for {}", client_id)
                else:
                    logger.info("Successfully optimized producer configuration")

        except Exception as e:
            # If any error occurs in our logic, log it and create the producer normally
            logger.error("[ERR-303] Failed to optimize producer with client id {}: {}", client_id, str(e))
            return orig_init(self, *args, **kwargs)

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
        if not bootstrap:
            return orig_init(self, conf, *args, **kwargs)
        bootstrap = normalize_bootstrap(bootstrap)
        client_id = conf.get("client.id", "")
        if client_id.startswith(_SUPERLIB_PREFIX):
            return orig_init(self, conf, *args, **kwargs)

        try:
            topics_env = get_topics_list()
            metadata = fetch_metadata(bootstrap, conf, "confluent")
            error_msg = ""
            if metadata is None:
                error_msg = "[ERR-304] Failed to fetch metadata for producer with client id {}: Unable to connect to Superstream service".format(client_id)
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            elif not metadata.active:
                error_msg = "[ERR-301] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it."
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            else:
                # Get optimized configuration if Superstream is active
                opt_cfg = optimal_cfg(metadata, topics_env, conf, "confluent")
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
                error=error_msg,  # Store error message in tracker
                metadata=metadata,
                topics_env=topics_env,
            )
            Heartbeat.register_tracker(tr)
            orig_init(self, conf, *args, **kwargs)
            if not hasattr(self, "_superstream_patch"):
                original_produce = self.produce

                def produce_patch(topic, *a, **kw):
                    tr.record_topic(topic)
                    return original_produce(topic, *a, **kw)

                self.produce = produce_patch
                orig_close = self.close

                def close_patch(*a, **kw):
                    if not hasattr(self, "_superstream_closed"):
                        self._superstream_closed = True
                        tr.close()
                        Heartbeat.unregister_tracker(tr.uuid)
                    return orig_close(*a, **kw)

                self.close = close_patch
                self._superstream_patch = True
            send_clients_msg(tr, error_msg)
            
            # Log success message based on whether defaults were used
            if not opt_cfg:  # No optimization applied
                pass  # Skip success message as there was an error
            elif all(opt_cfg.get(k) == v for k, v in _DEFAULTS.items()) and len(opt_cfg) == len(_DEFAULTS):  # Default optimization
                if client_id:
                    logger.info("Successfully optimized producer with default optimization parameters for {}", client_id)
                else:
                    logger.info("Successfully optimized producer with default optimization parameters")
            else:  # Custom optimization
                if client_id:
                    logger.info("Successfully optimized producer configuration for {}", client_id)
                else:
                    logger.info("Successfully optimized producer configuration")

        except Exception as e:
            # If any error occurs in our logic, log it and create the producer normally
            logger.error("[ERR-303] Failed to optimize producer with client id {}: {}", client_id, str(e))
            return orig_init(self, conf, *args, **kwargs)

    Producer.__init__ = init_patch 