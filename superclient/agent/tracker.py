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

    def record_topic(self, topic: str):
        """Record a topic that this producer writes to."""
        self.topics.add(topic)

    def close(self):
        """Mark this tracker as inactive."""
        self.active = False

    def determine_topic(self) -> str:
        """Get the most impactful topic for this producer."""
        return sorted(self.topics)[0] if self.topics else ""


class Heartbeat(threading.Thread):
    """Background thread that sends periodic heartbeats for all active producers."""
    
    _singleton: Optional["Heartbeat"] = None
    _lock = threading.Lock()
    _trackers: Dict[str, ProducerTracker] = {}
    _track_lock = threading.RLock()

    def __init__(self):
        super().__init__(name="superstream-heartbeat", daemon=True)
        self._stop = threading.Event()

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
        while not self._stop.is_set():
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
                send_clients_msg(tr)
                tr.last_hb = now
            time.sleep(1) 