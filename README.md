<div align="center">

<img src="https://github.com/user-attachments/assets/35899c78-24eb-4507-97ed-e87e84c49fea#gh-dark-mode-only" width="300">
<img src="https://github.com/user-attachments/assets/8a7bca49-c362-4a8c-945e-a331fb26d8eb#gh-light-mode-only" width="300">

</div>

# Superstream Client For Python

A Python library for automatically optimizing Kafka producer configurations based on topic-specific recommendations.

## Overview

Superstream Clients works as a Python import hook that intercepts Kafka producer creation and applies optimized configurations without requiring any code changes in your application. It dynamically retrieves optimization recommendations from Superstream and applies them based on impact analysis.

## Supported Libraries

Works with any Python library that implements Kafka producers, including:

- kafka-python
- aiokafka
- confluent-kafka
- Faust
- FastAPI event publishers
- Celery Kafka backends
- Any custom wrapper around these Kafka clients

## Features

- **Zero-code integration**: No code changes required in your application
- **Dynamic configuration**: Applies optimized settings based on topic-specific recommendations
- **Intelligent optimization**: Identifies the most impactful topics to optimize
- **Graceful fallback**: Falls back to default settings if optimization fails
- **Minimal overhead**: Uses a single lightweight background thread (or async coroutine for aiokafka)

## Important: Producer Configuration Requirements

When initializing your Kafka producers, please ensure you pass the configuration as a mutable object. The Superstream library needs to modify the producer configuration to apply optimizations. The following initialization patterns are supported:

✅ **Supported (Recommended)**:
```python
# Using kafka-python
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    compression_type='snappy',
    batch_size=16384
)

# Using aiokafka
from aiokafka import AIOKafkaProducer
producer = AIOKafkaProducer(
    bootstrap_servers='localhost:9092',
    compression_type='snappy',
    batch_size=16384
)

# Using confluent-kafka
from confluent_kafka import Producer
producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'compression.type': 'snappy',
    'batch.size': 16384
})
```

❌ **Not Supported**:
```python
# Using frozen dictionaries or immutable configurations
from types import MappingProxyType
config = MappingProxyType({
    'bootstrap.servers': 'localhost:9092'
})
producer = KafkaProducer(**config)
```

### Why This Matters
The Superstream library needs to modify your producer's configuration to apply optimizations based on your cluster's characteristics. This includes adjusting settings like compression, batch size, and other performance parameters. When the configuration is immutable, these optimizations cannot be applied.

## Installation

### Step 1: Install Superclient

```bash
pip install superclient
```

### Step 2: Run

The package ships with a `sitecustomize.py` entry-point, therefore Python imports the agent automatically before your application's code starts. This is the recommended and default way to use Superclient.

#### Manual Initialization (Only if needed)

If `sitecustomize` is disabled in your environment (e.g., when using `python -S` or when `PYTHONNOUSERSITE` is set), you can initialize manually by adding this import at the very beginning of your application's main entry point (e.g., `main.py`, `app.py`, or `__init__.py`):

```python
import superclient  # side-effects automatically enable the agent

# Your application code follows
from kafka import KafkaProducer
# ... rest of your imports and code
```

Note: The manual import must be placed before any Kafka-related imports to ensure proper interception of producer creation.

### Docker Integration

When using Superstream Clients with containerized applications, include the package in your Dockerfile:

```dockerfile
FROM python:3.8-slim

# Install superclient
RUN pip install superclient

# Your application code
COPY . /app
WORKDIR /app

# Run your application
CMD ["python", "your_app.py"]
```

### Required Environment Variables

- `SUPERSTREAM_TOPICS_LIST`: Comma-separated list of topics your application produces to

### Optional Environment Variables

- `SUPERSTREAM_LATENCY_SENSITIVE`: Set to "true" to prevent any modification to linger.ms values
- `SUPERSTREAM_DISABLED`: Set to "true" to disable optimization
- `SUPERSTREAM_DEBUG`: Set to "true" to enable debug logs

Example:
```bash
export SUPERSTREAM_TOPICS_LIST=orders,payments,user-events
export SUPERSTREAM_LATENCY_SENSITIVE=true
```

### SUPERSTREAM_LATENCY_SENSITIVE Explained

The linger.ms parameter follows these rules:

1. If SUPERSTREAM_LATENCY_SENSITIVE is set to true:
   - Linger value will never be modified, regardless of other settings

2. If SUPERSTREAM_LATENCY_SENSITIVE is set to false or not set:
   - If no explicit linger exists in original configuration: Use Superstream's optimized value
   - If explicit linger exists: Use the maximum of original value and Superstream's optimized value

## Prerequisites

- Python 3.8 or higher
- Kafka cluster that is connected to the Superstream's console
- Read and write permissions to the `superstream.*` topics

## License

This project is licensed under the Apache License 2.0. 