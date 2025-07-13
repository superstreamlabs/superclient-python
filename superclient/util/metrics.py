"""Metrics collection functionality for Kafka producers."""

import time
import math
import threading
import json
from typing import Any, Dict, Optional

from .logger import get_logger

logger = get_logger("util.metrics")


# Registry to store metrics extractors for each producer (defined after class)
_metrics_extractors: Dict[str, Any] = {}
_extractors_lock = threading.Lock()


def sanitize_metric_value(value: Any) -> Any:
    """Sanitize metric values to handle infinity, NaN, and other problematic values.
    
    Args:
        value: The metric value to sanitize
        
    Returns:
        Sanitized value that can be safely serialized
    """
    if value is None:
        return None
    
    # Handle infinity values
    if isinstance(value, (int, float)):
        if math.isinf(value):
            if value > 0:
                return "infinity"
            else:
                return "-infinity"
        elif math.isnan(value):
            return "nan"
    
    # Handle other numeric types that might have infinity
    try:
        float_val = float(value)
        if math.isinf(float_val):
            if float_val > 0:
                return "infinity"
            else:
                return "-infinity"
        elif math.isnan(float_val):
            return "nan"
    except (ValueError, TypeError):
        pass
    
    return value


def collect_kafka_python_metrics(producer: Any) -> Dict[str, Any]:
    """Collect metrics from kafka-python producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        # Extract producer metrics from the specific field
        producer_metrics = {}
        
        # Look for the producer-metrics field specifically
        if 'producer-metrics' in metrics:
            metric_data = metrics['producer-metrics']
            if isinstance(metric_data, dict):
                # Sanitize all metric values
                producer_metrics = {k: sanitize_metric_value(v) for k, v in metric_data.items()}
        
        return producer_metrics
    except Exception as e:
        logger.error("[ERR-306] Failed to collect kafka-python producer metrics: {}", e)
        return {}


def collect_confluent_producer_metrics(tracker_uuid: str) -> Dict[str, Any]:
    """Collect metrics from confluent-kafka producer using metrics extractor."""
    try:
        extractor = get_producer_metrics_extractor(tracker_uuid)
        if not extractor:
            logger.debug("No metrics extractor found for tracker {}", tracker_uuid)
            return {}
        
        producer_metrics, _, _ = extractor.get_metrics()
        return producer_metrics
    except Exception as e:
        logger.error("[ERR-307] Failed to collect confluent-kafka producer metrics: {}", e)
        return {}


def collect_aiokafka_metrics(producer: Any) -> Dict[str, Any]:
    """Collect metrics from aiokafka producer."""
    try:
        # aiokafka producers don't have a metrics() method
        # Return empty dict as aiokafka doesn't provide metrics
        return {}
    except Exception as e:
        logger.error("[ERR-308] Failed to collect aiokafka producer metrics: {}", e)
        return {}


def collect_kafka_python_topic_metrics(producer: Any) -> Dict[str, Any]:
    """Collect topic metrics from kafka-python producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        topic_metrics = {}
        
        # Look for topic-specific metrics fields
        for metric_name, metric_data in metrics.items():
            if metric_name.startswith('producer-topic-metrics'):
                # Extract topic name from metric name (e.g., "producer-topic-metrics.topic-name")
                parts = metric_name.split('.')
                if len(parts) >= 2:
                    topic_name = parts[1]
                    
                    if topic_name not in topic_metrics:
                        topic_metrics[topic_name] = {}
                    
                    if isinstance(metric_data, dict):
                        # Sanitize all metric values
                        topic_metrics[topic_name] = {k: sanitize_metric_value(v) for k, v in metric_data.items()}
        
        return topic_metrics
    except Exception as e:
        logger.error("[ERR-310] Failed to collect kafka-python topic metrics: {}", e)
        return {}


def collect_confluent_topic_metrics(tracker_uuid: str) -> Dict[str, Any]:
    """Collect topic metrics from confluent-kafka producer using metrics extractor."""
    try:
        extractor = get_producer_metrics_extractor(tracker_uuid)
        if not extractor:
            logger.debug("No metrics extractor found for tracker {}", tracker_uuid)
            return {}
        
        _, topic_metrics, _ = extractor.get_metrics()
        return topic_metrics
    except Exception as e:
        logger.error("[ERR-311] Failed to collect confluent topic metrics: {}", e)
        return {}


def collect_aiokafka_topic_metrics(producer: Any) -> Dict[str, Any]:
    """Collect topic metrics from aiokafka producer."""
    try:
        # aiokafka producers don't have a metrics() method
        # Return empty dict as aiokafka doesn't provide topic metrics
        return {}
    except Exception as e:
        logger.error("[ERR-312] Failed to collect aiokafka topic metrics: {}", e)
        return {}


def collect_kafka_python_node_metrics(producer: Any) -> Dict[str, Any]:
    """Collect node metrics from kafka-python producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        node_metrics = {}
        
        # Look for node-specific metrics fields
        for metric_name, metric_data in metrics.items():
            if metric_name.startswith('producer-node-metrics'):
                # Extract node ID from metric name (e.g., "producer-node-metrics.node-1")
                parts = metric_name.split('.')
                if len(parts) >= 2:
                    node_id = parts[1]
                    
                    # Filter out bootstrap node metrics
                    if node_id.startswith('node-bootstrap'):
                        continue
                    
                    # Remove "node-" prefix if present
                    if node_id.startswith('node-'):
                        node_id = node_id[5:]  # Remove "node-" prefix
                    
                    if node_id not in node_metrics:
                        node_metrics[node_id] = {}
                    
                    if isinstance(metric_data, dict):
                        # Sanitize all metric values
                        node_metrics[node_id] = {k: sanitize_metric_value(v) for k, v in metric_data.items()}
        
        return node_metrics
    except Exception as e:
        logger.error("[ERR-314] Failed to collect kafka-python node metrics: {}", e)
        return {}


def collect_confluent_node_metrics(tracker_uuid: str) -> Dict[str, Any]:
    """Collect node metrics from confluent-kafka producer using metrics extractor."""
    try:
        extractor = get_producer_metrics_extractor(tracker_uuid)
        if not extractor:
            logger.debug("No metrics extractor found for tracker {}", tracker_uuid)
            return {}
        
        _, _, node_metrics = extractor.get_metrics()
        return node_metrics
    except Exception as e:
        logger.error("[ERR-315] Failed to collect confluent node metrics: {}", e)
        return {}


def collect_aiokafka_node_metrics(producer: Any) -> Dict[str, Any]:
    """Collect node metrics from aiokafka producer."""
    try:
        # aiokafka producers don't have a metrics() method
        # Return empty dict as aiokafka doesn't provide node metrics
        return {}
    except Exception as e:
        logger.error("[ERR-316] Failed to collect aiokafka node metrics: {}", e)
        return {}


def merge_metrics_with_cache(
    current_producer_metrics: Dict[str, Any],
    current_topic_metrics: Dict[str, Any], 
    current_node_metrics: Dict[str, Any],
    cached_producer_metrics: Dict[str, Any],
    cached_topic_metrics: Dict[str, Any],
    cached_node_metrics: Dict[str, Any]
) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Merge current metrics with cached metrics, using cached values as fallbacks.
    
    This function maintains the last known value of each individual metric.
    For example: if time 0 has metrics X and Y, and time 1 has X and Z,
    the result will have X from time 1, Y from time 0, and Z from time 1.
    """
    
    # Merge producer metrics - preserve last known value of each metric
    merged_producer_metrics = cached_producer_metrics.copy()
    merged_producer_metrics.update(current_producer_metrics)
    
    # Merge topic metrics (nested structure) - preserve last known value of each metric per topic
    merged_topic_metrics = cached_topic_metrics.copy()
    for topic_name, topic_metrics in current_topic_metrics.items():
        if topic_name not in merged_topic_metrics:
            merged_topic_metrics[topic_name] = {}
        else:
            # Preserve existing cached metrics for this topic
            merged_topic_metrics[topic_name] = merged_topic_metrics[topic_name].copy()
        # Update with current metrics (preserves last known value of each metric)
        merged_topic_metrics[topic_name].update(topic_metrics)
    
    # Merge node metrics (nested structure) - preserve last known value of each metric per node
    merged_node_metrics = cached_node_metrics.copy()
    for node_id, node_metrics in current_node_metrics.items():
        if node_id not in merged_node_metrics:
            merged_node_metrics[node_id] = {}
        else:
            # Preserve existing cached metrics for this node
            merged_node_metrics[node_id] = merged_node_metrics[node_id].copy()
        # Update with current metrics (preserves last known value of each metric)
        merged_node_metrics[node_id].update(node_metrics)
    
    return merged_producer_metrics, merged_topic_metrics, merged_node_metrics


def collect_all_metrics(producer: Any, library: str, tracker_uuid: str = None) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Collect all metrics from producer for the given library."""
    try:
        # Collect producer metrics
        if library == "kafka-python":
            producer_metrics = collect_kafka_python_metrics(producer)
            topic_metrics = collect_kafka_python_topic_metrics(producer)
            node_metrics = collect_kafka_python_node_metrics(producer)
        elif library == "confluent":
            if not tracker_uuid:
                logger.error("[ERR-320] tracker_uuid is required for confluent-kafka metrics collection")
                return {}, {}, {}
            producer_metrics = collect_confluent_producer_metrics(tracker_uuid)
            topic_metrics = collect_confluent_topic_metrics(tracker_uuid)
            node_metrics = collect_confluent_node_metrics(tracker_uuid)
        elif library == "aiokafka":
            producer_metrics = collect_aiokafka_metrics(producer)
            topic_metrics = collect_aiokafka_topic_metrics(producer)
            node_metrics = collect_aiokafka_node_metrics(producer)
        else:
            producer_metrics = {}
            topic_metrics = {}
            node_metrics = {}
        
        return producer_metrics, topic_metrics, node_metrics
        
    except Exception as e:
        logger.error("[ERR-317] Failed to collect all metrics for {}: {}", library, e)
        return {}, {}, {}


class ConfluentMetricsExtractor:
    """Metrics extractor for confluent-kafka producers via stats_cb callback."""
    
    def __init__(self, producer_id: str = "unknown"):
        self.producer_id = producer_id
        self.lock = threading.Lock()
        self.producer_metrics = {}
        self.topic_metrics = {}
        self.node_metrics = {}
    
    def stats_cb(self, stats_json_str: str) -> None:
        """Callback function called by confluent-kafka with stats JSON string."""
        try:
            stats = json.loads(stats_json_str)
            
            with self.lock:
                # Extract producer metrics from root level
                self.producer_metrics = self._extract_producer_metrics(stats)
                
                # Extract topic metrics (pass producer metrics for compression rate)
                if 'topics' in stats:
                    self.topic_metrics = self._extract_topic_metrics(stats['topics'], self.producer_metrics)
                
                # Extract broker/node metrics
                if 'brokers' in stats:
                    self.node_metrics = self._extract_node_metrics(stats['brokers'])
                # log the record send total of producer metrics
        except Exception as e:
            logger.error("[ERR-317] Failed to parse confluent-kafka stats for producer {}: {}", self.producer_id, e)
    
    def _extract_producer_metrics(self, producer_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Extract producer-level metrics from confluent-kafka stats."""
        metrics = {}
        
        # Map confluent-kafka metric names to our standard java-based names
        metric_mapping = {
            'tx': 'request-total',
            'tx_bytes': 'outgoing-byte-total', # this is the size after compression
            'txmsgs': 'record-send-total',
            'txmsg_bytes': 'txmsg_bytes', # this is the size before compression                                                                                        
        }
        
        for confluent_name, standard_name in metric_mapping.items():
            if confluent_name in producer_stats:
                value = producer_stats[confluent_name]
                metrics[standard_name] = sanitize_metric_value(value)
        
        # Calculate compression rate: tx_bytes / txmsg_bytes (tx_bytes is the size after compression, txmsg_bytes is the size before compression)
        if 'tx_bytes' in producer_stats and 'txmsg_bytes' in producer_stats:
            tx_bytes = producer_stats['tx_bytes']
            txmsg_bytes = producer_stats['txmsg_bytes']
            
            if txmsg_bytes and txmsg_bytes > 0:
                compression_rate = tx_bytes / txmsg_bytes
                metrics['compression-rate-avg'] = sanitize_metric_value(compression_rate)
        
        # Calculate record size average: txmsg_bytes / txmsgs
        if 'txmsg_bytes' in producer_stats and 'txmsgs' in producer_stats:
            txmsg_bytes = producer_stats['txmsg_bytes']
            txmsgs = producer_stats['txmsgs']
            
            if txmsgs and txmsgs > 0:
                record_size_avg = int(txmsg_bytes / txmsgs)
                metrics['record-size-avg'] = sanitize_metric_value(record_size_avg)
        
        return metrics
    
    def _extract_topic_metrics(self, topics_stats: Dict[str, Any], producer_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Extract topic-level metrics from confluent-kafka stats."""
        topic_metrics = {}
        
        for topic_name, topic_data in topics_stats.items():
            if isinstance(topic_data, dict) and 'partitions' in topic_data:
                topic_metrics[topic_name] = {}
                
                # Initialize topic-level aggregators
                topic_txmsgs = 0
                topic_txbytes = 0
                
                # Iterate over partitions with partition ID >= 0
                for partition_id, partition_data in topic_data['partitions'].items():
                    try:
                        partition_num = int(partition_id)
                        if partition_num >= 0 and isinstance(partition_data, dict):
                            # Sum up partition metrics to topic level
                            if 'txmsgs' in partition_data:
                                topic_txmsgs += partition_data['txmsgs']
                            if 'txbytes' in partition_data:
                                topic_txbytes += partition_data['txbytes']
                    except (ValueError, TypeError):
                        # Skip non-numeric partition IDs
                        continue
                
                # Set topic-level metrics with java naming
                topic_metrics[topic_name]['record-send-total'] = sanitize_metric_value(topic_txmsgs)
                
                # Use producer-level compression rate for all topics since we don't have txmsg_bytes at topic level to calculate it
                if 'compression-rate-avg' in producer_metrics:
                    topic_metrics[topic_name]['compression-rate'] = producer_metrics['compression-rate-avg']
                    
                    # Calculate byte-total as txbytes * compression-rate since we don't have native txmsg_bytes at topic level
                    # This gives us the uncompressed byte total for the topic
                    byte_total = topic_txbytes * producer_metrics['compression-rate-avg']
                    topic_metrics[topic_name]['byte-total'] = sanitize_metric_value(byte_total)
                
                # Calculate topic-level record size average: txbytes / txmsgs (txbytes is the size before compression, txmsgs is the number of records)
                if topic_txmsgs > 0:
                    record_size_avg = int(topic_txbytes / topic_txmsgs)
                    topic_metrics[topic_name]['record-size-avg'] = sanitize_metric_value(record_size_avg)
        
        return topic_metrics
    
    def _extract_node_metrics(self, brokers_stats: Dict[str, Any]) -> Dict[str, Any]:
        """Extract broker/node-level metrics from confluent-kafka stats."""
        node_metrics = {}
        
        for broker_id, broker_data in brokers_stats.items():
            if isinstance(broker_data, dict):
                # Extract broker ID from format like "localhost:9092/1" -> "1"
                if '/' in broker_id:
                    clean_broker_id = broker_id.split('/')[-1]
                else:
                    clean_broker_id = broker_id
                
                node_metrics[clean_broker_id] = {}
                
                # Map confluent-kafka broker metric names to our standard names
                metric_mapping = {
                    'tx': 'request-total',
                    'txbytes': 'outgoing-byte-total', # this is the size after compression
                }
                
                for confluent_name, standard_name in metric_mapping.items():
                    if confluent_name in broker_data:
                        value = broker_data[confluent_name]
                        node_metrics[clean_broker_id][standard_name] = sanitize_metric_value(value)
        
        return node_metrics
    
    def get_metrics(self) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """Get the current metrics."""
        with self.lock:
            return (
                self.producer_metrics.copy(),
                self.topic_metrics.copy(),
                self.node_metrics.copy()
            )


# Update registry type annotation after class definition
_metrics_extractors: Dict[str, ConfluentMetricsExtractor] = {}


def configure_confluent_stats_callback(producer_config: Dict[str, Any], tracker_uuid: str) -> Dict[str, Any]:
    """Configure confluent-kafka producer with metrics extractor via stats callback.
    
    Args:
        producer_config: The producer configuration dictionary
        tracker_uuid: The unique UUID of the Superstream tracker
        
    Returns:
        Updated configuration with metrics extractor enabled via stats callback
    """
    try:
        # Create a new metrics extractor instance for this producer
        producer_id = producer_config.get('client.id', 'unknown')
        extractor = ConfluentMetricsExtractor(producer_id)
        
        # Store the extractor in our registry using tracker UUID (unique per producer instance)
        with _extractors_lock:
            _metrics_extractors[tracker_uuid] = extractor
        
        # Check if user already has a stats_cb configured
        user_stats_cb = producer_config.get('stats_cb')
        
        if user_stats_cb:
            # User has their own stats_cb - create a chained callback
            def chained_stats_cb(stats_json_str: str) -> None:
                """Chained callback that calls both user's callback and our metrics extractor."""
                try:
                    # Call user's original callback first
                    user_stats_cb(stats_json_str)
                except Exception as e:
                    logger.error("[ERR-319] User's stats_cb failed for producer {}: {}", producer_id, e)
                
                # Then call our metrics extractor
                extractor.stats_cb(stats_json_str)
            
            # Use the chained callback
            producer_config['stats_cb'] = chained_stats_cb
            logger.debug("Chained user's stats_cb with metrics extractor for producer {} (tracker: {})", producer_id, tracker_uuid)
        else:
            # No user callback - use our extractor directly
            producer_config['stats_cb'] = extractor.stats_cb
            logger.debug("Configured metrics extractor as stats_cb for producer {} (tracker: {})", producer_id, tracker_uuid)
        
        # Set stats interval (default: 5 seconds)
        if 'statistics.interval.ms' not in producer_config:
            producer_config['statistics.interval.ms'] = 5000
        
        logger.debug("Configured confluent-kafka metrics extractor for producer {} (tracker: {}) with {}ms stats interval", 
                   producer_id, tracker_uuid, producer_config.get('statistics.interval.ms', 5000))
        
        return producer_config
    except Exception as e:
        logger.error("[ERR-318] Failed to configure confluent-kafka metrics extractor: {}", e)
        return producer_config


def get_producer_metrics_extractor(tracker_uuid: str) -> Optional[ConfluentMetricsExtractor]:
    """Get the metrics extractor instance for a specific producer using tracker UUID."""
    try:
        # Get the extractor from our registry using tracker UUID
        with _extractors_lock:
            return _metrics_extractors.get(tracker_uuid)
    except Exception:
        return None


def remove_producer_metrics_extractor(tracker_uuid: str) -> None:
    """Remove a metrics extractor from the registry when producer is closed."""
    with _extractors_lock:
        _metrics_extractors.pop(tracker_uuid, None)
        logger.debug("Removed metrics extractor for tracker {}", tracker_uuid) 