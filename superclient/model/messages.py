"""Message models for Superstream client communication."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from superclient import __version__ as _LIB_VERSION

@dataclass
class TopicConfiguration:
    """Configuration for a specific topic."""
    topic_name: str
    optimized_configuration: Dict[str, Any]
    potential_reduction_percentage: float
    daily_writes_bytes: int

@dataclass
class MetadataMessage:
    """Message for metadata communication."""
    topics_configuration: List[TopicConfiguration]
    report_interval_ms: Optional[int] = None
    active: bool = True

@dataclass
class ClientMessage:
    """Base message for client communication."""
    client_id: str
    ip_address: str
    type: str = "producer"
    message_type: str = "client_stats"
    version: str = field(default_factory=lambda: _LIB_VERSION)
    topics: List[str] = field(default_factory=list)
    original_configuration: Dict[str, Any] = field(default_factory=dict)
    optimized_configuration: Dict[str, Any] = field(default_factory=dict)
    environment_variables: Dict[str, str] = field(default_factory=dict)
    hostname: str = ""
    superstream_client_uid: str = ""
    most_impactful_topic: str = ""
    language: str = ""
    error: str = ""