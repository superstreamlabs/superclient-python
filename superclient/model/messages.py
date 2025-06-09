"""Message models for Superstream client communication."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class TopicConfiguration:
    """Configuration for a specific topic."""
    topic_name: str
    optimized_configuration: Dict[str, Any]
    potential_reduction_percentage: float
    daily_writes_bytes: int

@dataclass
class ClientMessage:
    """Base message for client communication."""
    client_id: str
    ip_address: str
    type: str = "producer"
    message_type: str = "client_info"
    version: str = "0.1.0"
    topics: List[str] = field(default_factory=list)
    original_configuration: Dict[str, Any] = field(default_factory=dict)
    optimized_configuration: Dict[str, Any] = field(default_factory=dict)
    environment_variables: Dict[str, str] = field(default_factory=dict)
    hostname: str = ""
    superstream_client_uid: str = ""
    most_impactful_topic: str = ""
    language: str = "Python"
    error: str = ""

@dataclass
class ClientStatsMessage(ClientMessage):
    """Message for client statistics."""
    message_type: str = "client_stats"

@dataclass
class MetadataMessage:
    """Message for metadata communication."""
    topics_configuration: List[TopicConfiguration]
    report_interval_ms: Optional[int] = None 