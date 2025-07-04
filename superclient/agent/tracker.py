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

    def record_topic(self, topic: str):
        """Record a topic that this producer writes to."""
        self.topics.add(topic)

    def close(self):
        """Mark this tracker as inactive."""
        self.active = False

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
        """Main heartbeat loop."""
        while not self._stop_event.is_set():
            now = time.time()
            with self._track_lock:
                trackers = list(self._trackers.values())
            for tr in trackers:
                if not tr.active:
                    continue
                if (now - tr.last_hb) * 1000 < tr.report_interval_ms:
                    continue
                # Send heartbeat message
                from ..core.reporter import send_clients_msg
                send_clients_msg(tr, tr.error)
                tr.last_hb = now
            time.sleep(1) 