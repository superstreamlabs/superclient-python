"""Configuration utility functions."""

import os
import socket
from typing import Any, Dict, List

from .logger import get_logger

logger = get_logger("util.config")

def get_env_vars() -> Dict[str, str]:
    """Get Superstream environment variables."""
    return {k: v for k, v in os.environ.items() if k.startswith("SUPERSTREAM_")}

def is_disabled() -> bool:
    """Check if Superstream is disabled."""
    return os.getenv("SUPERSTREAM_DISABLED", "false").lower() == "true"

def is_debug_enabled() -> bool:
    """Check if debug mode is enabled."""
    return os.getenv("SUPERSTREAM_DEBUG", "false").lower() == "true"

def is_latency_sensitive() -> bool:
    """Check if latency sensitive mode is enabled."""
    return os.getenv("SUPERSTREAM_LATENCY_SENSITIVE", "false").lower() == "true"

def get_topics_list() -> List[str]:
    """Get list of topics from environment variable."""
    return [t.strip() for t in os.getenv("SUPERSTREAM_TOPICS_LIST", "").split(",") if t.strip()]

def mask_sensitive(k: str, v: Any) -> Any:
    """Mask sensitive configuration values."""
    sensitive_patterns = [
        "password", "sasl.jaas.config", "basic.auth.user.info",
        "ssl.key", "ssl.cert", "ssl.truststore", "ssl.keystore",
        "sasl.kerberos.keytab", "sasl.kerberos.principal"
    ]
    return "[MASKED]" if any(pattern in k.lower() for pattern in sensitive_patterns) else v

def _serialize_config_value(v: Any) -> Any:
    """Convert configuration values to JSON-serializable format."""
    if callable(v):
        # Convert functions to string representation
        return f"<function: {v.__name__ if hasattr(v, '__name__') else str(v)}>"
    elif hasattr(v, '__dict__'):
        # Handle objects that might have __dict__ but aren't functions
        try:
            # Try to serialize as dict, fallback to string representation
            return v.__dict__
        except:
            return f"<object: {type(v).__name__}>"
    else:
        return v

def copy_client_configuration_properties(src: Dict[str, Any], dst: Dict[str, Any], lib_name: str = "confluent"):
    """Copy essential client configuration properties from source to destination.
    This ensures internal Kafka clients have the same security, network, and connection
    configurations as the user's Kafka clients.
    Only copies properties that are explicitly set in the source configuration.
    
    Args:
        src: Source configuration (in library-specific syntax)
        dst: Destination configuration (in library-specific syntax)
        lib_name: Library name for key translation
    """
    # List of all possible auth/network related configs in Java-style syntax
    possible_keys = [
        # Security protocol
        "security.protocol",

        # SSL properties
        "ssl.truststore.location", "ssl.truststore.password",
        "ssl.keystore.location", "ssl.keystore.password",
        "ssl.key.password", "ssl.endpoint.identification.algorithm",
        "ssl.truststore.type", "ssl.keystore.type", "ssl.secure.random.implementation",
        "ssl.enabled.protocols", "ssl.cipher.suites", "ssl.protocol",
        "ssl.trustmanager.algorithm", "ssl.keymanager.algorithm",
        "enable.ssl.certificate.verification",
        "ssl.certificate.location", "ssl.certificate.pem",
        "ssl.ca.location", "ssl.ca.pem",
        "ssl.ca.certificate.stores", "ssl.crl.location",
        "ssl.providers", "ssl.context",
        "ssl.cafile", "ssl.certfile", "ssl.keyfile",

        # SASL properties
        "sasl.mechanism", "sasl.mechanisms", "sasl.jaas.config",
        "sasl.client.callback.handler.class", "sasl.login.callback.handler.class",
        "sasl.login.class", "sasl.kerberos.service.name",
        "sasl.kerberos.kinit.cmd", "sasl.kerberos.ticket.renew.window.factor",
        "sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.min.time.before.relogin",
        "sasl.kerberos.principal", "sasl.kerberos.keytab",
        "sasl.login.refresh.window.factor", "sasl.login.refresh.window.jitter",
        "sasl.login.refresh.min.period.seconds", "sasl.login.refresh.buffer.seconds",
        "sasl.login.retry.backoff.ms", "sasl.login.retry.backoff.max.ms",
        "sasl.username", "sasl.password",
        "sasl.plain.username", "sasl.plain.password",
        "sasl.oauthbearer.config", "sasl.oauthbearer.client.id",
        "sasl.oauthbearer.client.secret", "sasl.oauthbearer.scope",
        "sasl.oauthbearer.extensions", "sasl.oauthbearer.token.endpoint.url",
        "sasl.oauthbearer.scope.claim.name", "sasl.oauthbearer.sub.claim.name",
        "sasl.oauthbearer.clock.skew.seconds", "sasl.oauthbearer.jwks.endpoint.refresh.ms",
        "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms", "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms",
        "enable.sasl.oauthbearer.unsecure.jwt", "sasl.oauthbearer.method",

        # OAuth callback configurations (for MSK IAM and other OAuth providers)
        "sasl.oauth.token.provider",  # aiokafka, kafka-python
        "oauth_cb",  # confluent-kafka
        "sasl_oauth_token_provider",  # aiokafka underscore version

        # Network and connection properties
        "request.timeout.ms", "retry.backoff.ms", "connections.max.idle.ms",
        "reconnect.backoff.ms", "reconnect.backoff.max.ms",
        "client.dns.lookup",
        "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms",
        "retries"
    ]

    # For each Java-style key, find if it exists in the source config
    # by checking both the Java-style key and its library-specific equivalent
    for java_key in possible_keys:
        # Check if the Java-style key exists in source
        if java_key in src:
            if java_key not in dst:
                dst[java_key] = src[java_key]
            continue
            
        # Check if the library-specific equivalent exists in source
        if lib_name in _JAVA_TO_LIB_MAPPING:
            lib_key = _JAVA_TO_LIB_MAPPING[lib_name].get(java_key, java_key)
            if lib_key in src and lib_key not in dst:
                dst[lib_key] = src[lib_key]

# ---------------------------------------------------------------------------
# Field name mapping between Java-style and library-specific representations
# ---------------------------------------------------------------------------

# Mapping from Java-style field names to library-specific field names
_JAVA_TO_LIB_MAPPING: Dict[str, Dict[str, str]] = {
    "kafka-python": {
        # Basic configuration
        "bootstrap.servers": "bootstrap_servers",
        "client.id": "client_id",
        "key.serializer": "key_serializer",
        "value.serializer": "value_serializer",
        
        # Producer specific
        "enable.idempotence": "enable_idempotence",
        "delivery.timeout.ms": "delivery_timeout_ms",
        "compression.type": "compression_type",
        "batch.size": "batch_size",
        "linger.ms": "linger_ms",
        "connections.max.idle.ms": "connections_max_idle_ms",
        "max.block.ms": "max_block_ms",
        "max.request.size": "max_request_size",
        "allow.auto.create.topics": "allow_auto_create_topics",
        "metadata.max.age.ms": "metadata_max_age_ms",
        "retry.backoff.ms": "retry_backoff_ms",
        "request.timeout.ms": "request_timeout_ms",
        "receive.buffer.bytes": "receive_buffer_bytes",
        "send.buffer.bytes": "send_buffer_bytes",
        "reconnect.backoff.ms": "reconnect_backoff_ms",
        "reconnect.backoff.max.ms": "reconnect_backoff_max_ms",
        "max.in.flight.requests.per.connection": "max_in_flight_requests_per_connection",
        "security.protocol": "security_protocol",
        "ssl.check.hostname": "ssl_check_hostname",
        "api.version": "api_version",
        "api.version.auto.timeout.ms": "api_version_auto_timeout_ms",
        "metrics.enabled": "metrics_enabled",
        "metrics.num.samples": "metrics_num_samples",
        "metrics.sample.window.ms": "metrics_sample_window_ms",
        "sasl.mechanism": "sasl_mechanism",
        "sasl.plain.username": "sasl_plain_username",
        "sasl.plain.password": "sasl_plain_password",
        "sasl.kerberos.name": "sasl_kerberos_name",
        "sasl.kerberos.service.name": "sasl_kerberos_service_name",
        "sasl.kerberos.domain.name": "sasl_kerberos_domain_name",
        "sasl.oauth.token.provider": "sasl_oauth_token_provider",
        "socks5.proxy": "socks5_proxy",
        "ssl.cafile": "ssl_cafile",
        "ssl.certfile": "ssl_certfile", 
        "ssl.keyfile": "ssl_keyfile",
    },
    "aiokafka": {
        # Basic configuration
        "bootstrap.servers": "bootstrap_servers",
        "client.id": "client_id",
        "key.serializer": "key_serializer",
        "value.serializer": "value_serializer",
        
        # Producer specific
        "compression.type": "compression_type",
        "batch.size": "max_batch_size",  # aiokafka uses max_batch_size
        "linger.ms": "linger_ms",
        "max.request.size": "max_request_size",
        "metadata.max.age.ms": "metadata_max_age_ms",
        "request.timeout.ms": "request_timeout_ms",
        "retry.backoff.ms": "retry_backoff_ms",
        "api.version": "api_version",
        "connections.max.idle.ms": "connections_max_idle_ms",
        "enable.idempotence": "enable_idempotence",
        "security.protocol": "security_protocol",
        "sasl.mechanism": "sasl_mechanism",
    },
    "confluent": {
        # Confluent uses Java-style names directly, so most mappings are 1:1
        # But some fields have different names or don't exist
        "batch.size": "batch.size",
        "linger.ms": "linger.ms",
        "compression.type": "compression.type",
        "client.id": "client.id",
        "bootstrap.servers": "bootstrap.servers",
        "enable.idempotence": "enable.idempotence",
        "connections.max.idle.ms": "connections.max.idle.ms",
        "max.request.size": "message.max.bytes",  # Different field name
        "metadata.max.age.ms": "metadata.max.age.ms",
        "retry.backoff.ms": "retry.backoff.ms",
        "request.timeout.ms": "socket.timeout.ms",  # Different field name
        "reconnect.backoff.ms": "reconnect.backoff.ms",
        "reconnect.backoff.max.ms": "reconnect.backoff.max.ms",
        "max.in.flight.requests.per.connection": "max.in.flight.requests.per.connection",
        "security.protocol": "security.protocol",
        "api.version": "api.version.request",  # Different field name
        "sasl.mechanism": "sasl.mechanism",
        "allow.auto.create.topics": "allow.auto.create.topics",
        "receive.buffer.bytes": "socket.receive.buffer.bytes",  # Different field name
        "send.buffer.bytes": "socket.send.buffer.bytes",  # Different field name
        "api.version.auto.timeout.ms": "api.version.request.timeout.ms",  # Different field name
        "sasl.kerberos.service.name": "sasl.kerberos.service.name",
        
        # Confluent-specific fields
        "message.copy.max.bytes": "message.copy.max.bytes",
        "receive.message.max.bytes": "receive.message.max.bytes",
        "queue.buffering.max.messages": "queue.buffering.max.messages",
        "queue.buffering.max.kbytes": "queue.buffering.max.kbytes",
        "queue.buffering.max.ms": "queue.buffering.max.ms",
        "message.send.max.retries": "message.send.max.retries",
        "batch.num.messages": "batch.num.messages",
        "compression.codec": "compression.codec",
    }
}

# Reverse mapping from library-specific field names to Java-style field names
_LIB_TO_JAVA_MAPPING: Dict[str, Dict[str, str]] = {
    "kafka-python": {v: k for k, v in _JAVA_TO_LIB_MAPPING["kafka-python"].items()},
    "aiokafka": {v: k for k, v in _JAVA_TO_LIB_MAPPING["aiokafka"].items()},
    "confluent": {v: k for k, v in _JAVA_TO_LIB_MAPPING["confluent"].items()},
}

def translate_java_to_lib(cfg: Dict[str, Any], lib_name: str) -> Dict[str, Any]:
    """Translate Java-style configuration keys to library-specific keys."""
    if lib_name not in _JAVA_TO_LIB_MAPPING:
        return dict(cfg)
    
    mapping = _JAVA_TO_LIB_MAPPING[lib_name]
    translated = {}
    
    for key, value in cfg.items():
        # Use mapping if available, otherwise keep original key
        translated_key = mapping.get(key, key)
        # Skip fields that are not supported (mapped to None)
        if translated_key is not None:
            translated[translated_key] = value
    
    return translated

def translate_lib_to_java(cfg: Dict[str, Any], lib_name: str) -> Dict[str, Any]:
    """Translate library-specific configuration keys to Java-style keys."""
    if lib_name not in _LIB_TO_JAVA_MAPPING:
        return dict(cfg)
    
    mapping = _LIB_TO_JAVA_MAPPING[lib_name]
    translated = {}
    
    for key, value in cfg.items():
        # Use mapping if available, otherwise keep original key
        translated_key = mapping.get(key, key)
        # Skip fields that are not supported (mapped to None)
        if translated_key is not None:
            translated[translated_key] = value
    
    return translated

# ---------------------------------------------------------------------------
# Configuration helpers for library-specific syntax handling
# ---------------------------------------------------------------------------

_DEFAULT_CONFIGS: Dict[str, Dict[str, Any]] = {
    "kafka-python": {
        # Basic configuration
        "bootstrap_servers": "",
        "client_id": "",
        "key_serializer": None,
        "value_serializer": None,
        
        # Producer specific
        "enable_idempotence": False,
        "delivery_timeout_ms": 120000,
        "acks": 1,
        "compression_type": "none",
        "retries": 0,
        "batch_size": 16384,
        "linger_ms": 0,
        "partitioner": None,
        "connections_max_idle_ms": 540000,
        "max_block_ms": 60000,
        "max_request_size": 1048576,
        "allow_auto_create_topics": True,
        "metadata_max_age_ms": 300000,
        "retry_backoff_ms": 100,
        "request_timeout_ms": 30000,
        "receive_buffer_bytes": None,
        "send_buffer_bytes": None,
        "reconnect_backoff_ms": 50,
        "reconnect_backoff_max_ms": 30000,
        "max_in_flight_requests_per_connection": 5,
        
        # Security
        "security_protocol": "PLAINTEXT",
        "ssl_check_hostname": True,
        
        # API Version
        "api_version": None,
        "api_version_auto_timeout_ms": 2000,
        
        # Metrics
        "metrics_enabled": True,
        "metrics_num_samples": 2,
        "metrics_sample_window_ms": 30000,
        
        # SASL
        "sasl_mechanism": "PLAIN",
        
        # Other
        "socks5_proxy": None,
        "kafka_client": None,
    },
    "aiokafka": {
        # Basic configuration
        "bootstrap_servers": "",
        "client_id": "",
        "key_serializer": None,
        "value_serializer": None,
        
        # Producer specific
        "acks": 1,
        "compression_type": "none",
        "max_batch_size": 16384,  # aiokafka uses max_batch_size
        "linger_ms": 0,
        "partitioner": None,
        "max_request_size": 1048576,
        "metadata_max_age_ms": 300000,
        "request_timeout_ms": 40000,
        "retry_backoff_ms": 100,
        "api_version": "auto",
        "connections_max_idle_ms": 540000,
        "enable_idempotence": False,
        
        # Security
        "security_protocol": "PLAINTEXT",
        
        # SASL
        "sasl_mechanism": "PLAIN",
    },
    "confluent": {
        # Basic configuration
        "bootstrap.servers": "",
        "client.id": "",
        
        # Producer specific
        "message.max.bytes": 1000000,
        "message.copy.max.bytes": 65535,
        "receive.message.max.bytes": 100000000,
        "max.in.flight.requests.per.connection": 1000000,
        "enable.idempotence": False,
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 1048576,
        "queue.buffering.max.ms": 5,
        "linger.ms": 5,
        "message.send.max.retries": 2147483647,
        "retries": 2147483647,
        "retry.backoff.ms": 100,
        "retry.backoff.max.ms": 1000,
        "compression.codec": "none",
        "compression.type": "none",
        "batch.num.messages": 10000,
        "batch.size": 1000000,
        "delivery.report.only.error": False,
        "sticky.partitioning.linger.ms": 10,
        
        # Network
        "socket.timeout.ms": 60000,
        "socket.send.buffer.bytes": 0,
        "socket.receive.buffer.bytes": 0,
        "socket.keepalive.enable": False,
        "socket.nagle.disable": True,
        "socket.max.fails": 1,
        "broker.address.ttl": 1000,
        "broker.address.family": "any",
        "socket.connection.setup.timeout.ms": 30000,
        "connections.max.idle.ms": 0,
        "reconnect.backoff.ms": 100,
        "reconnect.backoff.max.ms": 10000,
        "client.dns.lookup": "use_all_dns_ips",
        
        # Security
        "security.protocol": "PLAINTEXT",
        "enable.ssl.certificate.verification": True,
        "ssl.endpoint.identification.algorithm": "https",
        
        # SASL
        "sasl.mechanisms": "GSSAPI",
        "sasl.mechanism": "GSSAPI",
        "sasl.oauthbearer.method": "default",
        
        # API Version
        "api.version.request": True,
        "api.version.request.timeout.ms": 10000,
        "api.version.fallback.ms": 0,
        "broker.version.fallback": "0.10.0",
        
        # Metrics
        "statistics.interval.ms": 0,
        "enable.metrics.push": True,
        
        # Client Rack
        "client.rack": None
    },
}

def get_default_configs(lib_name: str) -> Dict[str, Any]:
    """Return a *copy* of the default configuration dictionary for the given lib."""
    return dict(_DEFAULT_CONFIGS.get(lib_name, {}))


def get_original_config(orig_cfg: Dict[str, Any], lib_name: str) -> Dict[str, Any]:
    """Return the *complete* configuration (original + defaults) in Java-style syntax.

    Any parameter provided explicitly by the user overrides the defaults.  All
    keys are returned using Java-style syntax (e.g. `bootstrap.servers`).
    
    The function ensures that:
    1. User config is converted to Java-style syntax
    2. Defaults are in Java-style syntax for comparison
    3. Only truly missing configs get default values
    4. User values are preserved when they exist
    5. Final result is in Java-style syntax for reporting
    """
    # Get defaults in library-specific syntax
    defaults = get_default_configs(lib_name)
    
    # Convert user config from library-specific to Java-style syntax
    user_cfg_java = translate_lib_to_java(orig_cfg, lib_name)
    
    # Convert defaults to Java-style syntax
    defaults_java = translate_lib_to_java(defaults, lib_name)
    
    # Create a set of user config keys in Java-style syntax for comparison
    user_keys_java = set(user_cfg_java.keys())
    
    # Only add defaults for keys that are truly missing
    merged: Dict[str, Any] = dict(user_cfg_java)
    for k, v in defaults_java.items():
        if k not in user_keys_java:
            merged[k] = v
    
    # Serialize any function objects to make them JSON-serializable
    serialized: Dict[str, Any] = {}
    for k, v in merged.items():
        serialized[k] = _serialize_config_value(v)
            
    return serialized