import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("kafkapy.producer")

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json


# === Confluent Cloud connection config ===
BOOTSTRAP_SERVERS = ['your-cluster-name.region.aws.confluent.cloud:9092']
SASL_USERNAME = 'your-confluent-api-key'
SASL_PASSWORD = 'your-confluent-api-secret'

# === Producer Config ===
BATCH_SIZE = 150
LINGER_MS = 10
PRODUCER_NAME_1 = 'kafka-python-producer-1'
PRODUCER_NAME_2 = 'kafka-python-producer-2'
TOPICS_1 = ['example-topic', 'test1']
TOPICS_2 = ['example-topic', 'test2']

def create_producer(client_id):
    """Create and configure Kafka producer for Confluent Cloud using SASL_SSL"""
    logger.info(f"Creating producer for {client_id}")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=client_id,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        compression_type=None,
        batch_size=BATCH_SIZE,
        linger_ms=LINGER_MS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Send random JSON messages to specified Kafka topics"""
    successful = 0
    failed = 0
    
    logger.info(f"Sending {num_messages} messages from {producer_name} to {topics}")
    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            
            for topic in topics:
                future = producer.send(topic, value=message)
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
        logger.exception(f"Producer error: {e}")
    
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
