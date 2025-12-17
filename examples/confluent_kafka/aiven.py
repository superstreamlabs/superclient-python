import json
import time
import sys
import os
import logging
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("aiven.confluent_kafka.producer")

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
PRODUCER_NAME_1 = 'confluent-kafka-producer-1'
PRODUCER_NAME_2 = 'confluent-kafka-producer-2'
TOPICS_1 = ['example-topic', 'test-1']
TOPICS_2 = ['example-topic', 'test-2']

def create_producer(client_id):
    """Create and configure Confluent Kafka producer for Aiven using SSL"""
    logger.info(f"Creating Confluent Kafka producer with client_id={client_id}")
    return Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': client_id,
        'security.protocol': 'SSL',
        'ssl.ca.location': SSL_CAFILE,
        'ssl.certificate.location': SSL_CERTFILE,
        'ssl.key.location': SSL_KEYFILE,
        'batch.size': BATCH_SIZE,
        'linger.ms': LINGER_MS
    })

def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Send random JSON messages to specified Kafka topics"""
    logger.info(f"Sending {num_messages} messages using {producer_name} to topics: {topics}")
    successful = 0
    failed = 0

    def delivery_report(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            logger.error(f"Delivery failed for {msg.topic()}: {err}")

    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            key = f"msg-{i + 1}"
            encoded_value = json.dumps(message).encode('utf-8')

            for topic in topics:
                producer.produce(topic=topic, key=key.encode(), value=encoded_value, callback=delivery_report)

            successful += 1
        except Exception as e:
            failed += 1
            logger.exception(f"Failed to send message {i+1}: {e}")

        producer.poll(0)
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
        logger.info("Shutting down producers...")
        if producer1:
            logger.info("Producer 1 closed")
        if producer2:
            logger.info("Producer 2 closed")

    logger.info("Sleeping for 10 minutes...")
    time.sleep(600)
    logger.info("Sleep completed")

if __name__ == "__main__":
    main()
