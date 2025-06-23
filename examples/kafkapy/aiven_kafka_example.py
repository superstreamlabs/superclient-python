import json
import time
import sys
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("aiven.kafka.producer")

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
BATCH_SIZE = 150
LINGER_MS = 10
PRODUCER_NAME_1 = 'kafka-python-producer-1'
PRODUCER_NAME_2 = 'kafka-python-producer-2'
TOPICS_1 = ['example-topic', 'test-1']
TOPICS_2 = ['example-topic', 'test-2']

def create_producer(client_id):
    """Create and configure Kafka producer for Aiven using SSL"""
    logger.info(f"Creating KafkaProducer for client_id={client_id}")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=client_id,
        security_protocol="SSL",
        ssl_cafile=SSL_CAFILE,
        ssl_certfile=SSL_CERTFILE,
        ssl_keyfile=SSL_KEYFILE,
        compression_type=None,
        batch_size=BATCH_SIZE,
        linger_ms=LINGER_MS,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Send random JSON messages to specified Kafka topics"""
    logger.info(f"Sending {num_messages} messages using {producer_name} to topics: {topics}")
    successful = 0
    failed = 0

    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            key = f"msg-{i + 1}"

            for topic in topics:
                future = producer.send(topic, key=key, value=message)
                record_metadata = future.get(timeout=10)

            successful += 1
        except KafkaError as e:
            failed += 1
            logger.error(f"Failed to send message {i+1}: {e}")

        time.sleep(0.01)

    producer.flush()
    logger.info(f"{producer_name} Summary: {successful} successful, {failed} failed")

def main():
    producer1 = producer2 = None
    try:
        producer1 = create_producer(PRODUCER_NAME_1)
        producer2 = create_producer(PRODUCER_NAME_2)

        send_messages_to_topics(producer1, TOPICS_1, PRODUCER_NAME_1)
        send_messages_to_topics(producer2, TOPICS_2, PRODUCER_NAME_2)

    except Exception as e:
        logger.exception(f"Error during Kafka production: {e}")
    finally:
        if producer1:
            producer1.close()
            logger.info("Producer 1 closed")
        if producer2:
            producer2.close()
            logger.info("Producer 2 closed")

    logger.info("Sleeping for 10 minutes...")
    time.sleep(600)
    logger.info("Sleep completed")

if __name__ == "__main__":
    main()
