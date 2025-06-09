# Superstream Client for Python (`superclient`)

Superclient is a zero-code optimisation agent for Python applications that use Apache Kafka.  
It transparently intercepts producer creation in the popular client libraries and tunes
configuration parameters (compression, batching, etc.) based on recommendations
provided by the Superstream platform.

---

## Why use Superclient?

• **No code changes** – simply install the package and run your program.  
• **Dynamic configuration** – adapts to cluster-specific and topic-specific insights
  coming from `superstream.metadata_v1`.  
• **Safe by design** – any internal failure falls back to your original
  configuration; the application never crashes because of the agent.  
• **Minimal overhead** – uses a single lightweight background thread (or an async
  coroutine when running with `aiokafka`).

---

## Supported Kafka libraries

| Library | Producer class | Status |
|---------|----------------|--------|
| kafka-python | `kafka.KafkaProducer` | ✓ implemented |
| aiokafka | `aiokafka.AIOKafkaProducer` | ✓ implemented |
| confluent-kafka | `confluent_kafka.Producer` | ✓ implemented |

Other libraries/frameworks that wrap these producers (e.g. Faust, FastAPI event
publishers, Celery Kafka back-ends) inherit the benefits automatically.

---

## Installation

```bash
pip install superclient
```

The package ships with a `sitecustomize.py` entry-point, therefore Python
imports the agent automatically **before your application's code starts**.
If `sitecustomize` is disabled in your environment you can initialise manually:

```python
import superclient  # side-effects automatically enable the agent
```

---

## Environment variables

| Variable | Description |
|----------|-------------|
| `SUPERSTREAM_DISABLED` | `true` disables all functionality |
| `SUPERSTREAM_DEBUG` | `true` prints verbose debug logs |
| `SUPERSTREAM_TOPICS_LIST` | Comma-separated list of topics your application *may* write to |
| `SUPERSTREAM_LATENCY_SENSITIVE` | `true` prevents the agent from lowering `linger.ms` |

At start-up the agent logs the set of variables detected:

```
[superstream] INFO agent: Superstream Agent initialized with environment variables: {'SUPERSTREAM_DEBUG': 'true', ...}
```

---

## How it works

1. An **import hook** patches the producer classes once their modules are
   imported.
2. When your code creates a producer the agent:  
   a. Skips internal Superstream clients (their `client_id` starts with
      `superstreamlib-`).  
   b. Fetches the latest optimisation metadata from
      `superstream.metadata_v1`.  
   c. Computes an optimal configuration for the most impactful topic (or falls
      back to sensible defaults) while respecting the
      latency-sensitive flag.  
   d. Overrides producer kwargs/in-dict values before the original constructor
      executes.  
   e. Sends a *client_info* message to `superstream.clients` that contains both
      original and optimised configurations.
3. A single background heartbeat thread (or async task for `aiokafka`) emits
   *client_stats* messages every `report_interval_ms` (default 5 minutes).
4. When the application closes the producer the agent stops tracking it and
   ceases heart-beats.

---

## Logging

Log lines are printed to `stdout`/`stderr` and start with the `[superstream]`
prefix so they integrate with existing log pipelines.  Set
`SUPERSTREAM_DEBUG=true` for additional diagnostic messages.

---

## Security & compatibility

• Authentication/SSL/SASL/DNS settings are **copied from your original
  configuration** to every short-lived internal client.  
• The agent only relies on the Kafka library already present in your
  environment, therefore **no dependency conflicts** are introduced.  
• All exceptions are caught internally; your application will **never crash or
  hang** because of Superclient.

---

## License

Apache 2.0 