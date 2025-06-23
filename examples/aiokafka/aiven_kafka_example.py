import os
import sys
import json
import ssl
import time
import asyncio
import logging
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("aiven.aiokafka.producer")

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json

# === Aiven Kafka Bootstrap ===
BOOTSTRAP_SERVERS = ['your-service-name.aivencloud.com:PORT']

# === SSL Certificate Files from Aiven ZIP ===
SSL_CAFILE = "/path/to/ca.pem"               # Root CA file
SSL_CERTFILE = "/path/to/client.cert.pem"    # Client public certificate
SSL_KEYFILE = "/path/to/client.pk8.pem"      # Client private key


# === Producer Config ===
PRODUCER_NAME_1 = 'aiokafka-producer-1'
PRODUCER_NAME_2 = 'aiokafka-producer-2'
TOPICS_1 = ['example-topic', 'test-1']
TOPICS_2 = ['example-topic', 'test-2']


async def create_producer(client_id: str) -> AIOKafkaProducer:
    """Create and configure AIOKafkaProducer with SSL context"""
    logger.info(f"Creating AIOKafkaProducer for client_id={client_id}")

    ssl_ctx = ssl.create_default_context(cafile=SSL_CAFILE)
    ssl_ctx.load_cert_chain(certfile=SSL_CERTFILE, keyfile=SSL_KEYFILE)

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=client_id,
        security_protocol="SSL",
        ssl_context=ssl_ctx,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()
    return producer


async def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Send JSON messages to the given Kafka topics"""
    logger.info(f"Sending {num_messages} messages using {producer_name} to topics: {topics}")
    successful = 0
    failed = 0

    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message["message_number"] = i + 1
            message["producer"] = producer_name
            key = f"msg-{i + 1}"

            for topic in topics:
                await producer.send_and_wait(topic, key=key, value=message)

            successful += 1
        except Exception as e:
            failed += 1
            logger.error(f"Failed to send message {i+1}: {e}")

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
        logger.exception(f"Error during Kafka production: {e}")
    finally:
        logger.info("Shutting down producers...")
        if producer1:
            await producer1.stop()
            logger.info("Producer 1 closed")
        if producer2:
            await producer2.stop()
            logger.info("Producer 2 closed")

    logger.info("Sleeping for 10 minutes...")
    try:
        await asyncio.sleep(600)
    except asyncio.CancelledError:
        logger.info("Cancelled sleep")


if __name__ == "__main__":
    asyncio.run(main())
