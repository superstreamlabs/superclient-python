import asyncio
import json
import os
import ssl
import sys
import logging
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("confluent.aiokafka")

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json

# === Confluent Cloud Config ===
BOOTSTRAP_SERVERS = 'your-cluster-name.region.aws.confluent.cloud:9092'
SASL_USERNAME = 'your-confluent-api-key'
SASL_PASSWORD = 'your-confluent-api-secret'

# === Producer Config ===
PRODUCER_NAME_1 = 'aiokafka-confluent-producer-1'
PRODUCER_NAME_2 = 'aiokafka-confluent-producer-2'
TOPICS_1 = ['example-topic', 'test1']
TOPICS_2 = ['example-topic', 'test2']
BATCH_SIZE = 150
LINGER_MS = 10


async def create_producer(client_id):
    """Create a Confluent Cloud-compatible AIOKafkaProducer using SASL_SSL"""
    logger.info(f"Creating async producer for {client_id}")

    ssl_ctx = ssl.create_default_context()

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=client_id,
        security_protocol="SASL_SSL",
        ssl_context=ssl_ctx,
        sasl_mechanism="PLAIN",
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=LINGER_MS,
        max_batch_size=BATCH_SIZE,
    )
    await producer.start()
    return producer


async def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    successful = 0
    failed = 0

    logger.info(f"Sending {num_messages} messages from {producer_name} to {topics}")
    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            key = f"msg-{i + 1}"

            for topic in topics:
                await producer.send_and_wait(topic, key=key, value=message)

            successful += 1
        except Exception as e:
            failed += 1
            logger.error(f"Failed to send message {i + 1}: {e}")

        await asyncio.sleep(0.01)

    logger.info(f"{producer_name} Summary: {successful} successful, {failed} failed")


async def main():
    producer1 = producer2 = None
    try:
        producer1 = await create_producer(PRODUCER_NAME_1)
        producer2 = await create_producer(PRODUCER_NAME_2)

        await send_messages_to_topics(producer1, TOPICS_1, PRODUCER_NAME_1)
        await send_messages_to_topics(producer2, TOPICS_2, PRODUCER_NAME_2)

    except Exception as e:
        logger.exception(f"Producer error: {e}")
    finally:
        if producer1:
            await producer1.stop()
            logger.info("Producer 1 closed")
        if producer2:
            await producer2.stop()
            logger.info("Producer 2 closed")

    logger.info("Sleeping for 10 minutes...")
    await asyncio.sleep(600)


if __name__ == "__main__":
    asyncio.run(main())

