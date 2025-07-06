"""Producer tracking functionality."""

import threading
import time
import uuid
from typing import Any, Dict, Optional

from ..util.logger import get_logger

logger = get_logger("agent.tracker")

class ProducerTracker:
    """Tracks a Kafka producer and manages its heartbeat."""
    
    def __init__(
        self,
        lib: str,
        producer: Any,
        bootstrap: str,
        client_id: str,
        orig_cfg: Dict[str, Any],
        opt_cfg: Dict[str, Any],
        report_interval_ms: int,
        error: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        topics_env: Optional[list[str]] = None,
    ) -> None:
        self.uuid = str(uuid.uuid4())
        self.library = lib
        self.producer = producer
        self.bootstrap = bootstrap
        self.client_id = client_id
        self.orig_cfg = orig_cfg
        self.opt_cfg = opt_cfg
        self.topics: set[str] = set()
        self.report_interval_ms = report_interval_ms
        self.last_hb = 0.0
        self.active = True
        self.error = error
        self.metadata = metadata
        self.topics_env = topics_env or []
        self.start_time_ms = int(time.time() * 1000)  # Unix timestamp in milliseconds
        
        # Metrics cache for maintaining last known values
        self.producer_metrics_cache: Dict[str, Any] = {}
        self.topic_metrics_cache: Dict[str, Any] = {}
        self.node_metrics_cache: Dict[str, Any] = {}
        
        # Cache update tracking
        self.last_cache_update = 0.0
        self.cache_update_interval = 30.0  # Update cache every 10 seconds
        
        # Total tracking fields for cumulative metrics
        self.producer_totals = {
            'outgoing-byte-total': 0,
            'record-send-total': 0
        }
        self.topic_totals = {}  # topic_name -> {metric -> total}
        
        # Fractional tracking to avoid rounding errors
        self.producer_fractionals = {
            'outgoing-byte-total': 0.0,
            'record-send-total': 0.0
        }
        self.topic_fractionals = {}  # topic_name -> {metric -> fractional}
        
        # Compression rate tracking for preserving last known good values
        self.last_known_producer_compression_rate: Optional[float] = None
        self.last_known_topic_compression_rates: Dict[str, float] = {}
        
        # Record size tracking for preserving last known good values
        self.last_known_producer_record_size: Optional[float] = None

    def record_topic(self, topic: str):
        """Record a topic that this producer writes to."""
        self.topics.add(topic)

    def close(self):
        """Mark this tracker as inactive."""
        self.active = False

    def should_update_cache(self) -> bool:
        """Check if it's time to update the cache."""
        return time.time() - self.last_cache_update >= self.cache_update_interval

    def update_cache_if_needed(self):
        """Update cache if enough time has passed since last update."""
        if self.should_update_cache():
            self._update_cache_from_producer()
            self.last_cache_update = time.time()

    def _update_cache_from_producer(self):
        """Update cache with fresh metrics from producer using merge strategy and calculate totals."""
        try:
            from ..util.metrics import collect_all_metrics, merge_metrics_with_cache
            
            # Collect fresh metrics
            current_producer_metrics, current_topic_metrics, current_node_metrics = collect_all_metrics(self.producer, self.library)
            
            # Get current cached metrics
            cached_producer_metrics, cached_topic_metrics, cached_node_metrics = self.get_cached_metrics()
            
            # Calculate time difference since last update
            current_time = time.time()
            time_diff = current_time - self.last_cache_update if self.last_cache_update > 0 else 0
            
            # Calculate incremental totals from rates only if native totals are missing
            self._update_producer_totals_if_missing(current_producer_metrics, time_diff)
            
            # Calculate incremental totals from rates only if native totals are missing
            self._update_topic_totals_if_missing(current_topic_metrics, time_diff)
            
            # Merge fresh metrics with cache (fresh takes precedence, cache provides fallbacks)
            merged_producer_metrics, merged_topic_metrics, merged_node_metrics = merge_metrics_with_cache(
                current_producer_metrics, current_topic_metrics, current_node_metrics,
                cached_producer_metrics, cached_topic_metrics, cached_node_metrics
            )
            
            # Add calculated totals to merged metrics
            merged_producer_metrics.update(self.producer_totals)
            
            # Preserve last known good compression rates and record sizes when current is 0
            self._preserve_compression_rates(merged_producer_metrics, merged_topic_metrics)
            self._preserve_record_sizes(merged_producer_metrics)
            
            # Add topic totals to merged topic metrics
            for topic_name, totals in self.topic_totals.items():
                if topic_name in merged_topic_metrics:
                    merged_topic_metrics[topic_name].update(totals)
                else:
                    merged_topic_metrics[topic_name] = totals
            
            # Update cache with merged results (including totals)
            self.producer_metrics_cache = merged_producer_metrics
            self.topic_metrics_cache = merged_topic_metrics
            self.node_metrics_cache = merged_node_metrics
            
            logger.debug("Updated metrics cache for producer {} (merged fresh + cached + totals)", self.client_id)
        except Exception as e:
            logger.error("[ERR-319] Failed to collect metrics for cache update for {}: {}", self.client_id, e)

    def update_metrics_cache(self, producer_metrics: Dict[str, Any], topic_metrics: Dict[str, Any], node_metrics: Dict[str, Any]) -> None:
        """Update the metrics cache with new values."""
        self.producer_metrics_cache = producer_metrics.copy()
        self.topic_metrics_cache = topic_metrics.copy()
        self.node_metrics_cache = node_metrics.copy()

    def update_metrics_cache_selective(self, producer_metrics: Dict[str, Any], topic_metrics: Dict[str, Any], node_metrics: Dict[str, Any]) -> None:
        """Update the metrics cache selectively - only update non-empty individual metrics to preserve good cached values.
        
        This ensures that if we have metrics X and Y cached, and new collection returns only X and Z,
        we keep Y from cache, update X with new value, and add Z.
        """
        # Update producer metrics - only update non-empty individual metrics
        if producer_metrics:
            for metric_name, metric_value in producer_metrics.items():
                if metric_value is not None:  # Only update if metric has a value
                    self.producer_metrics_cache[metric_name] = metric_value
        
        # Update topic metrics - only update non-empty individual metrics per topic
        if topic_metrics:
            for topic_name, topic_metric_data in topic_metrics.items():
                if topic_name not in self.topic_metrics_cache:
                    self.topic_metrics_cache[topic_name] = {}
                if topic_metric_data:  # Only update if topic has metrics
                    for metric_name, metric_value in topic_metric_data.items():
                        if metric_value is not None:  # Only update if metric has a value
                            self.topic_metrics_cache[topic_name][metric_name] = metric_value
        
        # Update node metrics - only update non-empty individual metrics per node
        if node_metrics:
            for node_id, node_metric_data in node_metrics.items():
                if node_id not in self.node_metrics_cache:
                    self.node_metrics_cache[node_id] = {}
                if node_metric_data:  # Only update if node has metrics
                    for metric_name, metric_value in node_metric_data.items():
                        if metric_value is not None:  # Only update if metric has a value
                            self.node_metrics_cache[node_id][metric_name] = metric_value

    def _update_producer_totals_if_missing(self, producer_metrics: Dict[str, Any], time_diff: float) -> None:
        """Update producer-level totals by integrating rates over time, only if native totals are missing.
        
        These totals are calculated because they don't exist in native Kafka metrics.
        We integrate rate metrics (events/second) over time to get cumulative totals.
        Uses fractional tracking to avoid rounding errors at each step.
        """
        if time_diff <= 0:
            return
            
        # Map of rate metrics to their corresponding total fields
        rate_to_total_mapping = {
            'outgoing-byte-rate': 'outgoing-byte-total',
            'record-send-rate': 'record-send-total'
        }
        
        for rate_metric, total_metric in rate_to_total_mapping.items():
            # Only calculate if native total is missing
            if total_metric not in producer_metrics:
                if rate_metric in producer_metrics:
                    rate_value = producer_metrics[rate_metric]
                    if isinstance(rate_value, (int, float)) and rate_value > 0:
                        # Add to fractional accumulator to avoid rounding errors
                        incremental_fractional = rate_value * time_diff
                        self.producer_fractionals[total_metric] += incremental_fractional
                        
                        # Convert accumulated fractional to integer (round only once)
                        new_total = int(round(self.producer_fractionals[total_metric]))
                        old_total = self.producer_totals[total_metric]
                        actual_increment = new_total - old_total
                        
                        if actual_increment > 0:
                            self.producer_totals[total_metric] = new_total
                            logger.debug("Calculated missing {}: +{} (rate: {:.2f} * time: {:.2f}s, fractional: {:.3f})", 
                                       total_metric, actual_increment, rate_value, time_diff, self.producer_fractionals[total_metric])
            else:
                # Native total exists, use it instead of calculated total
                native_total = producer_metrics[total_metric]
                if isinstance(native_total, (int, float)) and native_total > 0:
                    self.producer_totals[total_metric] = int(native_total)
                    # Reset fractional to match native total
                    self.producer_fractionals[total_metric] = float(native_total)
                    logger.debug("Using native {}: {}", total_metric, native_total)

    def _update_topic_totals_if_missing(self, topic_metrics: Dict[str, Any], time_diff: float) -> None:
        """Update topic-level totals by integrating rates over time, only if native totals are missing.
        
        These totals are calculated because they don't exist in native Kafka metrics.
        We integrate rate metrics (events/second) over time to get cumulative totals.
        Uses fractional tracking to avoid rounding errors at each step.
        """
        if time_diff <= 0:
            return
            
        # Map of rate metrics to their corresponding total fields (topic level)
        rate_to_total_mapping = {
            'record-send-rate': 'record-send-total',
            'byte-rate': 'byte-total'
        }
        
        for topic_name, topic_metric_data in topic_metrics.items():
            if topic_name not in self.topic_totals:
                self.topic_totals[topic_name] = {
                    'record-send-total': 0,
                    'byte-total': 0
                }
            if topic_name not in self.topic_fractionals:
                self.topic_fractionals[topic_name] = {
                    'record-send-total': 0.0,
                    'byte-total': 0.0
                }
            
            for rate_metric, total_metric in rate_to_total_mapping.items():
                # Only calculate if native total is missing
                if total_metric not in topic_metric_data:
                    if rate_metric in topic_metric_data:
                        rate_value = topic_metric_data[rate_metric]
                        if isinstance(rate_value, (int, float)) and rate_value > 0:
                            # Add to fractional accumulator to avoid rounding errors
                            incremental_fractional = rate_value * time_diff
                            self.topic_fractionals[topic_name][total_metric] += incremental_fractional
                            
                            # Convert accumulated fractional to integer (round only once)
                            new_total = int(round(self.topic_fractionals[topic_name][total_metric]))
                            old_total = self.topic_totals[topic_name][total_metric]
                            actual_increment = new_total - old_total
                            
                            if actual_increment > 0:
                                self.topic_totals[topic_name][total_metric] = new_total
                                logger.debug("Calculated missing topic {} {}: +{} (rate: {:.2f} * time: {:.2f}s, fractional: {:.3f})", 
                                           topic_name, total_metric, actual_increment, rate_value, time_diff, 
                                           self.topic_fractionals[topic_name][total_metric])
                else:
                    # Native total exists, use it instead of calculated total
                    native_total = topic_metric_data[total_metric]
                    if isinstance(native_total, (int, float)) and native_total > 0:
                        self.topic_totals[topic_name][total_metric] = int(native_total)
                        # Reset fractional to match native total
                        self.topic_fractionals[topic_name][total_metric] = float(native_total)
                        logger.debug("Using native topic {} {}: {}", topic_name, total_metric, native_total)

    def _preserve_compression_rates(self, producer_metrics: Dict[str, Any], topic_metrics: Dict[str, Any]) -> None:
        """Preserve last known good compression rates when current values are 0.
        
        This prevents compression rates from being reset to 0 when no compression
        is happening temporarily (e.g., during idle periods).
        """
        # Handle producer-level compression rate
        if 'compression-rate-avg' in producer_metrics:
            current_rate = producer_metrics['compression-rate-avg']
            if isinstance(current_rate, (int, float)):
                if current_rate > 0:
                    # Update last known good value
                    self.last_known_producer_compression_rate = current_rate
                elif current_rate == 0 and self.last_known_producer_compression_rate is not None:
                    # Use last known good value when current is 0
                    producer_metrics['compression-rate-avg'] = self.last_known_producer_compression_rate
                    logger.debug("Preserved producer compression rate: {} (current was 0)", 
                               self.last_known_producer_compression_rate)
        
        # Handle topic-level compression rates
        for topic_name, topic_metric_data in topic_metrics.items():
            if 'compression-rate' in topic_metric_data:
                current_rate = topic_metric_data['compression-rate']
                if isinstance(current_rate, (int, float)):
                    if current_rate > 0:
                        # Update last known good value for this topic
                        self.last_known_topic_compression_rates[topic_name] = current_rate
                    elif current_rate == 0 and topic_name in self.last_known_topic_compression_rates:
                        # Use last known good value when current is 0
                        topic_metric_data['compression-rate'] = self.last_known_topic_compression_rates[topic_name]
                        logger.debug("Preserved topic {} compression rate: {} (current was 0)", 
                                   topic_name, self.last_known_topic_compression_rates[topic_name])

    def _preserve_record_sizes(self, producer_metrics: Dict[str, Any]) -> None:
        """Preserve last known good record size when current value is 0.
        
        This prevents record size from being reset to 0 when no records
        are being sent temporarily (e.g., during idle periods).
        """
        # Handle producer-level record size
        if 'record-size-avg' in producer_metrics:
            current_size = producer_metrics['record-size-avg']
            if isinstance(current_size, (int, float)):
                if current_size > 0:
                    # Update last known good value
                    self.last_known_producer_record_size = current_size
                elif current_size == 0 and self.last_known_producer_record_size is not None:
                    # Use last known good value when current is 0
                    producer_metrics['record-size-avg'] = self.last_known_producer_record_size
                    logger.debug("Preserved producer record size: {} (current was 0)", 
                               self.last_known_producer_record_size)

    def get_cached_metrics(self) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """Get the cached metrics."""
        return self.producer_metrics_cache.copy(), self.topic_metrics_cache.copy(), self.node_metrics_cache.copy()

    def determine_topic(self) -> str:
        """Get the most impactful topic for this producer based on metadata analysis."""
        if not self.metadata or not self.metadata.get("topics_configuration"):
            # Fallback to first topic if no metadata available
            return sorted(self.topics)[0] if self.topics else ""
        
        # Find matching topic configurations from metadata based on environment topics only
        matches = [
            tc for tc in self.metadata["topics_configuration"] 
            if tc["topic_name"] in self.topics_env
        ]
        
        if not matches:
            # Fallback to first environment topic if no matches
            return sorted(self.topics_env)[0] if self.topics_env else ""
        
        # Use the same logic as optimal_cfg: find the topic with highest impact
        best = max(matches, key=lambda tc: tc["potential_reduction_percentage"] * tc["daily_writes_bytes"])
        return best["topic_name"]


class Heartbeat(threading.Thread):
    """Background thread that sends periodic heartbeats for all active producers."""
    
    _singleton: Optional["Heartbeat"] = None
    _lock = threading.Lock()
    _trackers: Dict[str, ProducerTracker] = {}
    _track_lock = threading.RLock()

    def __init__(self):
        super().__init__(name="superstream-heartbeat", daemon=True)
        self._stop_event = threading.Event()

    @classmethod
    def ensure(cls):
        """Ensure the heartbeat thread is running."""
        with cls._lock:
            if cls._singleton is None or not cls._singleton.is_alive():
                cls._singleton = Heartbeat()
                cls._singleton.start()

    @classmethod
    def register_tracker(cls, tracker: ProducerTracker):
        """Register a new producer tracker."""
        with cls._track_lock:
            tracker.last_hb = time.time()  # Set initial timestamp to prevent immediate reporting
            cls._trackers[tracker.uuid] = tracker

    @classmethod
    def unregister_tracker(cls, tracker_id: str):
        """Unregister a producer tracker."""
        with cls._track_lock:
            cls._trackers.pop(tracker_id, None)

    def run(self):
        """Main heartbeat loop - handles both reporting and metrics cache updates."""
        while not self._stop_event.is_set():
            now = time.time()
            with self._track_lock:
                trackers = list(self._trackers.values())
            
            for tr in trackers:
                if not tr.active:
                    continue
                
                # Update cache if needed (every 30 seconds)
                try:
                    tr.update_cache_if_needed()
                except Exception as e:
                    logger.error("[ERR-318] Failed to update metrics cache for {}: {}", tr.client_id, e)
                
                # Send heartbeat if needed (every 5 minutes by default)
                if (now - tr.last_hb) * 1000 < tr.report_interval_ms:
                    continue
                try:
                    from ..core.reporter import send_clients_msg
                    send_clients_msg(tr, tr.error)
                    tr.last_hb = now
                except Exception as e:
                    logger.error("[ERR-320] Failed to send heartbeat for {}: {}", tr.client_id, e)
            
            time.sleep(1) 