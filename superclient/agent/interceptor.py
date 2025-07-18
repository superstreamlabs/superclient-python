"""Producer interception functionality."""

import os
import uuid
from typing import Any, Dict
import importlib

from ..util.logger import get_logger
from ..util.config import get_topics_list, is_disabled
from ..util.metrics import configure_confluent_stats_callback
from .metadata import fetch_metadata_sync, optimal_cfg, _DEFAULTS
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
        
        # Normalize compression type: convert None to "none" string
        if "compression_type" in orig_cfg and orig_cfg["compression_type"] is None:
            orig_cfg["compression_type"] = "none"
        
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
            metadata = fetch_metadata_sync(bootstrap, orig_cfg, "kafka-python")
            
            # Check if Superstream is active for this cluster
            error_msg = ""
            if metadata is None:
                # not logging as it is already logged in fetch_metadata
                error_msg = "[ERR-304] Failed to fetch metadata for producer with client id {}: Unable to connect to Superstream service".format(client_id)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            elif not metadata.get("active", True):
                error_msg = "[ERR-301] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it."
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            else:
                # Get optimized configuration if Superstream is active
                opt_cfg, warning_msg = optimal_cfg(metadata, topics_env, orig_cfg, "kafka-python")
                if warning_msg:
                    error_msg = warning_msg
            
            # Apply optimized configuration
            for k, v in opt_cfg.items():
                current_val = kwargs.get(k)
                if current_val != v:
                    if k in kwargs:
                        logger.debug("Overriding configuration: {} ({} -> {})", k, current_val, v)
                    else:
                        logger.debug("Overriding configuration: {} ((not set) -> {})", k, v)
                    kwargs[k] = v

            # Set up reporting interval
            report_interval = metadata.get("report_interval_ms") if metadata else _DEFAULT_REPORT_INTERVAL_MS
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
                        logger.debug("Superstream tracking stopped for kafka-python producer with client_id: {}", client_id)
                    return orig_close(*a, **kw)

                self.close = close_patch
                self._superstream_patch = True

            # Initialize with optimized configuration
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
        
        # Normalize compression type: convert None to "none" string
        if "compression_type" in orig_cfg and orig_cfg["compression_type"] is None:
            orig_cfg["compression_type"] = "none"
        
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
            metadata = fetch_metadata_sync(bootstrap, orig_cfg, "aiokafka")
            error_msg = ""
            if metadata is None:
                error_msg = "[ERR-304] Failed to fetch metadata for producer with client id {}: Unable to connect to Superstream service".format(client_id)
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            elif not metadata.get("active", True):
                error_msg = "[ERR-301] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it."
                logger.error(error_msg)
                # Skip optimization but keep stats reporting
                opt_cfg = {}
            else:
                # Get optimized configuration if Superstream is active
                opt_cfg, warning_msg = optimal_cfg(metadata, topics_env, orig_cfg, "aiokafka")
                if warning_msg:
                    error_msg = warning_msg
            for k, v in opt_cfg.items():
                current_val = kwargs.get(k)
                if current_val != v:
                    if k in kwargs:
                        logger.debug("Overriding configuration: {} ({} -> {})", k, current_val, v)
                    else:
                        logger.debug("Overriding configuration: {} ((not set) -> {})", k, v)
                    kwargs[k] = v
            report_interval = metadata.get("report_interval_ms") if metadata else _DEFAULT_REPORT_INTERVAL_MS
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
                        logger.debug("Superstream tracking stopped for aiokafka producer with client_id: {}", client_id)
                    await original_stop(*a, **kw)

                self.stop = stop_patch
                
                # Patch the SendProduceReqHandler.create_request method for this specific producer
                sender_mod = importlib.import_module("aiokafka.producer.sender")
                
                # Only patch once globally
                if not hasattr(sender_mod.SendProduceReqHandler, '_superstream_patched'):
                    orig_create_request = sender_mod.SendProduceReqHandler.create_request
                    
                    def create_request_with_metrics(self_handler):
                        # Call the original method to get the request, but collect metrics
                        # self_handler._batches: Dict[TopicPartition, MessageBatch]
                        
                        # Quick check: if sender has no tracker, it's an internal producer - skip metrics
                        if not hasattr(self_handler._sender, '_superstream_tracker'):
                            return orig_create_request(self_handler)
                            
                        tracker = self_handler._sender._superstream_tracker
                        
                        # Additional check: skip internal producers by client_id
                        if tracker is None or tracker.client_id.startswith(_SUPERLIB_PREFIX):
                            return orig_create_request(self_handler)
                            
                        # Per-producer totals
                        total_uncompressed = 0
                        total_compressed = 0
                        total_records = 0
                        topic_stats = {}
                        for tp, batch in self_handler._batches.items():
                            # Get record count from the batch
                            record_count = batch.record_count
                            
                            # Get compressed size from the batch buffer
                            compressed = 0
                            try:
                                compressed = len(batch.get_data_buffer())
                            except Exception:
                                pass
                            
                            # Estimate uncompressed size based on record count
                            # Since we can't easily access the original message data at this point,
                            # we'll use a reasonable estimate based on the batch size and record count
                            if record_count > 0:
                                # Estimate uncompressed size based on compressed size and typical compression ratios
                                # This is an approximation since we can't access the original message data
                                estimated_compression_ratio = 0.7  # Assume 30% compression
                                uncompressed = int(compressed / estimated_compression_ratio)
                            else:
                                uncompressed = 0
                            
                            total_uncompressed += uncompressed
                            total_compressed += compressed
                            total_records += record_count
                            # Per-topic
                            if tp.topic not in topic_stats:
                                topic_stats[tp.topic] = {'uncompressed': 0, 'compressed': 0, 'records': 0}
                            topic_stats[tp.topic]['uncompressed'] += uncompressed
                            topic_stats[tp.topic]['compressed'] += compressed
                            topic_stats[tp.topic]['records'] += record_count
                        # Update tracker
                        if total_records > 0:
                            tracker._superstream_metrics = getattr(tracker, '_superstream_metrics', {})
                            m = tracker._superstream_metrics
                            # Accumulate totals (aggregative counters)
                            m['outgoing-byte-total'] = m.get('outgoing-byte-total', 0) + total_compressed
                            m['record-send-total'] = m.get('record-send-total', 0) + total_records
                            m['uncompressed-byte-total'] = m.get('uncompressed-byte-total', 0) + total_uncompressed
                            
                            # Calculate rates from aggregated totals
                            m['compression-rate-avg'] = (m['outgoing-byte-total'] / m['uncompressed-byte-total']) if m['uncompressed-byte-total'] else 1.0
                            m['record-size-avg'] = (m['uncompressed-byte-total'] / m['record-send-total']) if m['record-send-total'] else 0
                            
                            # Per-topic
                            m['topics'] = m.get('topics', {})
                            for topic, stats in topic_stats.items():
                                t = m['topics'].setdefault(topic, {'byte-total': 0, 'record-send-total': 0, 'uncompressed-total': 0})
                                # Accumulate totals (aggregative counters)
                                t['byte-total'] = t.get('byte-total', 0) + stats['compressed']
                                t['record-send-total'] = t.get('record-send-total', 0) + stats['records']
                                t['uncompressed-total'] = t.get('uncompressed-total', 0) + stats['uncompressed']
                                
                                # Calculate compression rate from aggregated totals
                                t['compression-rate'] = (t['byte-total'] / t['uncompressed-total']) if t['uncompressed-total'] else 1.0
                                
                            tracker._superstream_metrics = m
                        return orig_create_request(self_handler)
                    
                    sender_mod.SendProduceReqHandler.create_request = create_request_with_metrics
                    sender_mod.SendProduceReqHandler._superstream_patched = True
                
                self._superstream_patch = True
            orig_init(self, *args, **kwargs)
            
                        # Store tracker reference in the sender for metrics collection
            if hasattr(self, '_sender'):
                self._sender._superstream_tracker = tr
            
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
    
    # Check if Producer exists and is not already patched
    if not hasattr(mod, "Producer"):
        logger.warn("confluent_kafka module does not have Producer class")
        return
        
    Producer = mod.Producer
    
    # Check if already patched
    if hasattr(mod, '_OriginalProducer'):
        logger.debug("confluent_kafka Producer already patched")
        return
    
    # Store the original Producer class
    mod._OriginalProducer = Producer
    
    # Create a wrapper class that provides the same interface as Producer
    class SuperstreamProducer:
        def __init__(self, conf: Dict[str, Any], *args, **kwargs):
            if is_disabled():
                self._producer = Producer(conf, *args, **kwargs)
                return
                
            conf = dict(conf)
            
            # Normalize compression type: convert None to "none" string
            if "compression.type" in conf and conf["compression.type"] is None:
                conf["compression.type"] = "none"
            
            bootstrap = conf.get("bootstrap.servers")
            if not bootstrap:
                self._producer = Producer(conf, *args, **kwargs)
                return
            bootstrap = normalize_bootstrap(bootstrap)
            client_id = conf.get("client.id", "")
            if client_id.startswith(_SUPERLIB_PREFIX):
                self._producer = Producer(conf, *args, **kwargs)
                return

            try:
                topics_env = get_topics_list()
                metadata = fetch_metadata_sync(bootstrap, conf, "confluent")
                error_msg = ""
                if metadata is None:
                    error_msg = "[ERR-304] Failed to fetch metadata for producer with client id {}: Unable to connect to Superstream service".format(client_id)
                    logger.error(error_msg)
                    # Skip optimization but keep stats reporting
                    opt_cfg = {}
                elif not metadata.get("active", True):
                    error_msg = "[ERR-301] Superstream optimization is not active for this kafka cluster, please head to the Superstream console and activate it."
                    logger.error(error_msg)
                    # Skip optimization but keep stats reporting
                    opt_cfg = {}
                else:
                    # Get optimized configuration if Superstream is active
                    opt_cfg, warning_msg = optimal_cfg(metadata, topics_env, conf, "confluent")
                    if warning_msg:
                        error_msg = warning_msg
                
                # Store original configuration before applying optimizations
                orig_cfg = dict(conf)
                
                # Apply optimizations to the configuration
                for k, v in opt_cfg.items():
                    current_val = conf.get(k)
                    if current_val != v:
                        if k in conf:
                            logger.debug("Overriding configuration: {} ({} -> {})", k, current_val, v)
                        else:
                            logger.debug("Overriding configuration: {} ((not set) -> {})", k, v)
                        conf[k] = v
                
                
                # Generate UUID for this producer
                tracker_uuid = str(uuid.uuid4())
                
                # Configure stats callback for metrics collection
                conf = configure_confluent_stats_callback(conf, tracker_uuid)
                
                # Create the producer with optimized configuration
                self._producer = Producer(conf, *args, **kwargs)
                
                report_interval = metadata.get("report_interval_ms") if metadata else _DEFAULT_REPORT_INTERVAL_MS
                # Create tracker with the generated UUID
                self._tracker = ProducerTracker(
                    lib="confluent",
                    producer=self._producer,
                    bootstrap=bootstrap,
                    client_id=client_id,
                    orig_cfg=orig_cfg,
                    opt_cfg=opt_cfg,
                    report_interval_ms=int(report_interval or _DEFAULT_REPORT_INTERVAL_MS),
                    error=error_msg,
                    metadata=metadata,
                    topics_env=topics_env,
                    uuid=tracker_uuid,  # Use the generated UUID
                )
                
                Heartbeat.register_tracker(self._tracker)
                
                send_clients_msg(self._tracker, error_msg)
                
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
                self._producer = Producer(conf, *args, **kwargs)
        
        def produce(self, topic, *args, **kwargs):
            """Wrapper for produce method that tracks topics."""
            if hasattr(self, '_tracker'):
                self._tracker.record_topic(topic)
            return self._producer.produce(topic, *args, **kwargs)
        
        def __del__(self):
            """Destructor to automatically clean up when producer is garbage collected."""
            if hasattr(self, '_tracker') and not hasattr(self, '_superstream_closed'):
                try:
                    self._superstream_closed = True
                    self._tracker.close()
                    Heartbeat.unregister_tracker(self._tracker.uuid)
                    
                    # Remove metrics extractor from registry
                    from ..util.metrics import remove_producer_metrics_extractor
                    remove_producer_metrics_extractor(self._tracker.uuid)
                    
                    logger.debug("Superstream tracking stopped for confluent-kafka producer with client_id: {}", 
                               getattr(self._tracker, 'client_id', 'unknown'))
                except Exception as e:
                    logger.error("Error during automatic cleanup: {}", e)
        
        def __getattr__(self, name):
            """Delegate all other attributes to the underlying producer."""
            return getattr(self._producer, name)
    
    # Replace the Producer class in the module
    mod.Producer = SuperstreamProducer 