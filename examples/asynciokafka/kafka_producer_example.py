import asyncio
import logging
from aiokafka import AIOKafkaProducer
import json
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducerExample")

# Configuration constants
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
CLIENT_ID = "superstream-example-producer"
TOPIC_NAME = "example-topic"
MESSAGE_KEY = "test-key"
RECORD_COUNT = 50

# Generate a compressible JSON message (~1KB)
def generate_large_compressible_message():
    message = {
        "metadata": {
            "id": "12345",
            "type": "example",
            "timestamp": 1635954438000
        },
        "data": {
            "metrics": [
                {
                    "name": f"metric{i}",
                    "value": i * 10,
                    "tags": ["tag1", "tag2", "tag3"],
                    "properties": {
                        "property1": "value1",
                        "property2": "value2"
                    }
                }
                for i in range(15)
            ]
        },
        "config": {
            "sampling": "full",
            "retention": "30d",
            "compression": True,
            "encryption": False
        }
    }
    return json.dumps(message)

async def send_messages():
    message_value = generate_large_compressible_message()

    producer = AIOKafkaProducer(
        bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
        client_id=CLIENT_ID,
        key_serializer=str.encode,
        value_serializer=str.encode,
        compression_type="snappy",  # Can be 'gzip', 'snappy', 'lz4', 'zstd'
        linger_ms=500,
        max_batch_size=10 * 1024
    )

    producer1 = AIOKafkaProducer(
        bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
        client_id=CLIENT_ID + "1",
        key_serializer=str.encode,
        value_serializer=str.encode,
        compression_type="snappy",
        linger_ms=500,
        max_batch_size=10 * 1024
    )

    await producer.start()
    await producer1.start()
    try:
        while True:
            for i in range(1, RECORD_COUNT + 1):
                message_key = f"{MESSAGE_KEY}-{i}"
                message_payload = f"{message_value}-{i}-{int(time.time() * 1000)}"

                await producer.send_and_wait(TOPIC_NAME, key=message_key, value=message_payload)
                await producer.send_and_wait(TOPIC_NAME + "1", key=message_key, value=message_payload)
                await producer1.send_and_wait(TOPIC_NAME + "1", key=message_key, value=message_payload)

                logger.info(f"Sent message {i} to all topics")
            await asyncio.sleep(100)
    except Exception as e:
        logger.error("Error sending message", exc_info=e)
    finally:
        await producer.stop()
        await producer1.stop()

if __name__ == "__main__":
    asyncio.run(send_messages())
