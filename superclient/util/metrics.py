"""Metrics collection functionality for Kafka producers."""

import time
import math
from typing import Any, Dict, Optional

from .logger import get_logger

logger = get_logger("util.metrics")


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


def collect_confluent_metrics(producer: Any) -> Dict[str, Any]:
    """Collect metrics from confluent-kafka producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        # Extract relevant producer metrics - flat structure
        producer_metrics = {}
        
        # Confluent metrics are organized by metric type
        for metric_type, metric_data in metrics.items():
            if metric_type == 'producer':
                for metric_name, metric_value in metric_data.items():
                    producer_metrics[metric_name] = sanitize_metric_value(metric_value)
            elif metric_type == 'broker':
                # Include broker metrics as they're relevant to producer performance
                for metric_name, metric_value in metric_data.items():
                    producer_metrics[metric_name] = sanitize_metric_value(metric_value)
        
        return producer_metrics
    except Exception as e:
        logger.error("[ERR-307] Failed to collect confluent-kafka producer metrics: {}", e)
        return {}


def collect_aiokafka_metrics(producer: Any) -> Dict[str, Any]:
    """Collect metrics from aiokafka producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        # Extract relevant producer metrics - flat structure
        producer_metrics = {}
        
        # aiokafka metrics structure is similar to kafka-python
        for metric_name, metric_data in metrics.items():
            # Look for producer-related metrics
            if any(keyword in metric_name.lower() for keyword in ['producer', 'record', 'batch', 'request', 'connection', 'network', 'io', 'buffer', 'compression']):
                # Extract the value from the metric data
                if hasattr(metric_data, 'value'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.value)
                elif isinstance(metric_data, dict) and 'value' in metric_data:
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data['value'])
                elif hasattr(metric_data, 'count'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.count)
                elif hasattr(metric_data, 'mean'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.mean)
                elif hasattr(metric_data, 'rate'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.rate)
                elif hasattr(metric_data, 'total'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.total)
                elif hasattr(metric_data, 'avg'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.avg)
                elif hasattr(metric_data, 'max'):
                    producer_metrics[metric_name] = sanitize_metric_value(metric_data.max)
                else:
                    producer_metrics[metric_name] = sanitize_metric_value(str(metric_data))
        
        return producer_metrics
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


def collect_confluent_topic_metrics(producer: Any) -> Dict[str, Any]:
    """Collect topic metrics from confluent-kafka producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        topic_metrics = {}
        
        # Look for topic-specific metrics in confluent format - nested structure
        for metric_type, metric_data in metrics.items():
            if metric_type == 'topic' and isinstance(metric_data, dict):
                # Confluent topic metrics are organized by topic name
                for topic_name, topic_metric_data in metric_data.items():
                    if isinstance(topic_metric_data, dict):
                        topic_metrics[topic_name] = {}
                        for metric_name, metric_value in topic_metric_data.items():
                            topic_metrics[topic_name][metric_name] = sanitize_metric_value(metric_value)
        
        return topic_metrics
    except Exception as e:
        logger.error("[ERR-311] Failed to collect confluent topic metrics: {}", e)
        return {}


def collect_aiokafka_topic_metrics(producer: Any) -> Dict[str, Any]:
    """Collect topic metrics from aiokafka producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        topic_metrics = {}
        
        # Look for topic-specific metrics - nested structure with topic names as keys
        for metric_name, metric_data in metrics.items():
            if 'topic' in metric_name.lower():
                # Extract topic name from metric name (e.g., "topic.my-topic.record-send-rate")
                parts = metric_name.split('.')
                if len(parts) >= 2 and parts[0] == 'topic':
                    topic_name = parts[1]
                    metric_key = '.'.join(parts[2:]) if len(parts) > 2 else 'value'
                    
                    if topic_name not in topic_metrics:
                        topic_metrics[topic_name] = {}
                    
                    # Extract the value from the metric data
                    if hasattr(metric_data, 'value'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.value)
                    elif isinstance(metric_data, dict) and 'value' in metric_data:
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data['value'])
                    elif hasattr(metric_data, 'count'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.count)
                    elif hasattr(metric_data, 'mean'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.mean)
                    elif hasattr(metric_data, 'rate'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.rate)
                    elif hasattr(metric_data, 'total'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.total)
                    elif hasattr(metric_data, 'avg'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.avg)
                    elif hasattr(metric_data, 'max'):
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(metric_data.max)
                    else:
                        topic_metrics[topic_name][metric_key] = sanitize_metric_value(str(metric_data))
        
        return topic_metrics
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


def collect_confluent_node_metrics(producer: Any) -> Dict[str, Any]:
    """Collect node metrics from confluent-kafka producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        node_metrics = {}
        
        # Look for node/broker-specific metrics in confluent format - nested structure
        for metric_type, metric_data in metrics.items():
            if metric_type == 'broker' and isinstance(metric_data, dict):
                # Confluent broker metrics are organized by broker ID
                for broker_id, broker_metric_data in metric_data.items():
                    # Filter out bootstrap node metrics
                    if broker_id.startswith('node-bootstrap'):
                        continue
                    
                    # Remove "node-" prefix if present
                    clean_broker_id = broker_id
                    if broker_id.startswith('node-'):
                        clean_broker_id = broker_id[5:]  # Remove "node-" prefix
                    
                    if isinstance(broker_metric_data, dict):
                        node_metrics[clean_broker_id] = {}
                        for metric_name, metric_value in broker_metric_data.items():
                            node_metrics[clean_broker_id][metric_name] = sanitize_metric_value(metric_value)
        
        return node_metrics
    except Exception as e:
        logger.error("[ERR-315] Failed to collect confluent node metrics: {}", e)
        return {}


def collect_aiokafka_node_metrics(producer: Any) -> Dict[str, Any]:
    """Collect node metrics from aiokafka producer."""
    try:
        metrics = producer.metrics()
        if not metrics:
            return {}
        
        node_metrics = {}
        
        # Look for node/broker-specific metrics - nested structure with node IDs as keys
        for metric_name, metric_data in metrics.items():
            if any(keyword in metric_name.lower() for keyword in ['node', 'broker', 'connection', 'network']):
                # Extract node ID from metric name (e.g., "node.8.request-rate")
                parts = metric_name.split('.')
                if len(parts) >= 2 and parts[0] in ['node', 'broker']:
                    node_id = parts[1]
                    
                    # Filter out bootstrap node metrics
                    if node_id.startswith('node-bootstrap'):
                        continue
                    
                    # Remove "node-" prefix if present
                    if node_id.startswith('node-'):
                        node_id = node_id[5:]  # Remove "node-" prefix
                    
                    metric_key = '.'.join(parts[2:]) if len(parts) > 2 else 'value'
                    
                    if node_id not in node_metrics:
                        node_metrics[node_id] = {}
                    
                    # Extract the value from the metric data
                    if hasattr(metric_data, 'value'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.value)
                    elif isinstance(metric_data, dict) and 'value' in metric_data:
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data['value'])
                    elif hasattr(metric_data, 'count'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.count)
                    elif hasattr(metric_data, 'mean'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.mean)
                    elif hasattr(metric_data, 'rate'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.rate)
                    elif hasattr(metric_data, 'total'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.total)
                    elif hasattr(metric_data, 'avg'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.avg)
                    elif hasattr(metric_data, 'max'):
                        node_metrics[node_id][metric_key] = sanitize_metric_value(metric_data.max)
                    else:
                        node_metrics[node_id][metric_key] = sanitize_metric_value(str(metric_data))
        
        return node_metrics
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


def collect_all_metrics(producer: Any, library: str) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Collect all metrics from producer for the given library."""
    try:
        # Collect producer metrics
        if library == "kafka-python":
            producer_metrics = collect_kafka_python_metrics(producer)
            topic_metrics = collect_kafka_python_topic_metrics(producer)
            node_metrics = collect_kafka_python_node_metrics(producer)
        elif library == "confluent":
            producer_metrics = collect_confluent_metrics(producer)
            topic_metrics = collect_confluent_topic_metrics(producer)
            node_metrics = collect_confluent_node_metrics(producer)
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