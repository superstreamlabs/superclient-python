"""Configuration utility functions."""

import os
import socket
from typing import Any, Dict, List

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
    return "[MASKED]" if "password" in k.lower() or "sasl.jaas.config" in k.lower() else v

def copy_client_configuration_properties(src: Dict[str, Any], dst: Dict[str, Any]):
    """Copy essential client configuration properties from source to destination.
    This ensures internal Kafka clients have the same security, network, and connection
    configurations as the user's Kafka clients.
    Only copies properties that are explicitly set in the source configuration.
    """
    # List of all possible auth/network related configs
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
        "sasl.oauthbearer.config", "sasl.oauthbearer.client.id",
        "sasl.oauthbearer.client.secret", "sasl.oauthbearer.scope",
        "sasl.oauthbearer.extensions", "sasl.oauthbearer.token.endpoint.url",
        "sasl.oauthbearer.scope.claim.name", "sasl.oauthbearer.sub.claim.name",
        "sasl.oauthbearer.clock.skew.seconds", "sasl.oauthbearer.jwks.endpoint.refresh.ms",
        "sasl.oauthbearer.jwks.endpoint.retry.backoff.ms", "sasl.oauthbearer.jwks.endpoint.retry.backoff.max.ms",
        "enable.sasl.oauthbearer.unsecure.jwt", "sasl.oauthbearer.method",

        # Network and connection properties
        "request.timeout.ms", "retry.backoff.ms", "connections.max.idle.ms",
        "reconnect.backoff.ms", "reconnect.backoff.max.ms",
        "client.dns.lookup",
        "socket.connection.setup.timeout.ms", "socket.connection.setup.timeout.max.ms",
        "retries"
    ]

    # Only copy properties that are explicitly set in the source configuration
    for k in possible_keys:
        if k in src and k not in dst:
            dst[k] = src[k]

# ---------------------------------------------------------------------------
# Configuration helpers for library-specific syntax handling
# ---------------------------------------------------------------------------

_DEFAULT_CONFIGS: Dict[str, Dict[str, Any]] = {
    "kafka-python": {
        # Basic configuration
        "bootstrap.servers": "",
        "client.id": "",
        "key.serializer": None,
        "value.serializer": None,
        
        # Producer specific
        "enable.idempotence": False,
        "delivery.timeout.ms": 120000,
        "acks": 1,
        "compression.type": None,
        "retries": float('inf'),
        "batch.size": 16384,
        "linger.ms": 0,
        "partitioner": None,
        "connections.max.idle.ms": 540000,
        "max.block.ms": 60000,
        "max.request.size": 1048576,
        "allow.auto.create.topics": True,
        "metadata.max.age.ms": 300000,
        "retry.backoff.ms": 100,
        "request.timeout.ms": 30000,
        "receive.buffer.bytes": 32768,
        "send.buffer.bytes": 131072,
        "socket.options": [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        "reconnect.backoff.ms": 50,
        "reconnect.backoff.max.ms": 30000,
        "max.in.flight.requests.per.connection": 5,
        
        # Security
        "security.protocol": "PLAINTEXT",
        "ssl.check.hostname": True,
        
        # API Version
        "api.version": None,
        "api.version.auto.timeout.ms": 2000,
        
        # Metrics
        "metric.reporters": [],
        "metrics.enabled": True,
        "metrics.num.samples": 2,
        "metrics.sample.window.ms": 30000,
        
        # SASL
        "sasl.mechanism": None,
        "sasl.kerberos.name": None,
        "sasl.kerberos.service.name": "kafka",
        "sasl.kerberos.domain.name": None,
        "sasl.oauth.token.provider": None,
        
        # Other
        "socks5.proxy": None,
        "kafka.client": None,
    },
    "aiokafka": {
        # Basic configuration
        "bootstrap.servers": "",
        "client.id": "",
        "key.serializer": None,
        "value.serializer": None,
        
        # Producer specific
        "acks": 1,
        "compression.type": None,
        "max.batch.size": 16384,
        "linger.ms": 0,
        "partitioner": None,
        "max.request.size": 1048576,
        "metadata.max.age.ms": 300000,
        "request.timeout.ms": 40000,
        "retry.backoff.ms": 100,
        "api.version": "auto",
        "connections.max.idle.ms": 540000,
        "enable.idempotence": False,
        
        # Security
        "security.protocol": "PLAINTEXT",
        
        # SASL
        "sasl.mechanism": "PLAIN",
        "sasl.oauth.token.provider": None,
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
        "sasl.kerberos.service.name": "kafka",
        "sasl.kerberos.principal": "kafkaclient",
        "enable.sasl.oauthbearer.unsecure.jwt": False,
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


def convert_to_dot_syntax(cfg: Dict[str, Any], lib_name: str) -> Dict[str, Any]:
    """Convert configuration keys to dot syntax regardless of original style."""
    if lib_name in ("kafka-python", "aiokafka"):
        return {k.replace("_", "."): v for k, v in cfg.items()}
    return dict(cfg)


def convert_from_dot_syntax(cfg: Dict[str, Any], lib_name: str) -> Dict[str, Any]:
    """Convert configuration keys from dot syntax to the appropriate library style."""
    if lib_name in ("kafka-python", "aiokafka"):
        return {k.replace(".", "_"): v for k, v in cfg.items()}
    return dict(cfg)


def get_default_configs(lib_name: str) -> Dict[str, Any]:
    """Return a *copy* of the default configuration dictionary for the given lib."""
    return dict(_DEFAULT_CONFIGS.get(lib_name, {}))


def get_original_config(orig_cfg: Dict[str, Any], lib_name: str) -> Dict[str, Any]:
    """Return the *complete* configuration (original + defaults) in dot syntax.

    Any parameter provided explicitly by the user overrides the defaults.  All
    keys are returned using dot syntax (e.g. `bootstrap.servers`).
    
    The function ensures that:
    1. Both user config and defaults are in dot syntax for comparison
    2. Only truly missing configs get default values
    3. User values are preserved when they exist
    """
    # Get defaults in dot syntax
    defaults = get_default_configs(lib_name)
    
    # Convert user config to dot syntax
    user_cfg_dot = convert_to_dot_syntax(orig_cfg, lib_name)
    
    # Create a set of user config keys in dot syntax for comparison
    user_keys_dot = set(user_cfg_dot.keys())
    
    # Only add defaults for keys that are truly missing
    merged: Dict[str, Any] = dict(user_cfg_dot)
    for k, v in defaults.items():
        if k not in user_keys_dot:
            merged[k] = v
            
    return merged 