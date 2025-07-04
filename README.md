<div align="center">

<img src="https://github.com/user-attachments/assets/35899c78-24eb-4507-97ed-e87e84c49fea#gh-dark-mode-only" width="300">
<img src="https://github.com/user-attachments/assets/8a7bca49-c362-4a8c-945e-a331fb26d8eb#gh-light-mode-only" width="300">

</div>

# Superclient Python

A Python library for automatically optimizing Kafka producer configurations based on topic-specific recommendations.

## Overview

Superstream Clients works as a Python import hook that intercepts Kafka producer creation and applies optimized configurations without requiring any code changes in your application. It dynamically retrieves optimization recommendations from Superstream and applies them based on impact analysis.

## Supported Libraries

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

## Installation

```bash
pip install superstream-clients && python -m superclient install_pth
```

That's it! Superclient will now automatically load and optimize all Kafka producers in your Python environment.

## Usage

After installation, superclient works automatically. Just use your Kafka clients as usual.

### Docker Integration

When using Superstream Clients with containerized applications, include the package in your Dockerfile:

```dockerfile
FROM python:3.8-slim

# Install superclient
RUN pip install superstream-clients
RUN python -m superclient install_pth

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

## Prerequisites

- Python 3.8 or higher
- Kafka cluster that is connected to the Superstream's console
- Read and write permissions to the `superstream.*` topics

## License

Apache License 2.0 