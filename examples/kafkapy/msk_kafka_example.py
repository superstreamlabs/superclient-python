import json
import time
import sys
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.getLogger("kafka").setLevel(logging.WARNING)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json 

# === AWS MSK IAM Kafka Connection Config ===
BOOTSTRAP_SERVERS = [
    'b-1-public.your-msk-cluster.amazonaws.com:9198',
    'b-2-public.your-msk-cluster.amazonaws.com:9198'
]

# AWS IAM credentials (
os.environ["AWS_ACCESS_KEY_ID"] = "<YOUR_AWS_ACCESS_KEY_ID>"
os.environ["AWS_SECRET_ACCESS_KEY"] = "<YOUR_AWS_SECRET_ACCESS_KEY>"

SECURITY_PROTOCOL = "SASL_SSL"
SASL_MECHANISM = "AWS_MSK_IAM"

# === Kafka Producer Config ===
BATCH_SIZE = 16384
LINGER_MS = 10
COMPRESSION_TYPE = "gzip"

PRODUCER_NAME_1 = "msk-python-producer-1"
PRODUCER_NAME_2 = "msk-python-producer-2"
TOPICS_1 = ["example-topic"]
TOPICS_2 = ["example-topic"]

def create_producer(client_id: str) -> KafkaProducer:
    logger.info(f"🔧 Creating KafkaProducer for client_id={client_id}")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=client_id,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanism=SASL_MECHANISM,
        compression_type=COMPRESSION_TYPE,
        batch_size=BATCH_SIZE,
        linger_ms=LINGER_MS,
    )


def send_messages_to_topics(producer: KafkaProducer, topics: list, producer_name: str, num_messages: int = 50):
    successful = 0
    failed = 0

    logger.info(f"Sending {num_messages} messages using {producer_name} to topics: {topics}")
    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            key = f"msg-{i+1}"

            for topic in topics:
                future = producer.send(topic=topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                logger.info(f"Sent message {i+1} to {record_metadata.topic} partition {record_metadata.partition}")

            successful += 1
        except KafkaError as ke:
            logger.error(f"KafkaError while sending message {i+1}: {ke}")
            failed += 1
        except Exception as e:
            logger.exception(f"Unexpected error while sending message {i+1}: {e}")
            failed += 1

        time.sleep(0.01)

    producer.flush()
    logger.info(f"\n {producer_name} Summary: {successful} successful, {failed} failed")


def main():
    producer1 = producer2 = None
    try:
        producer1 = create_producer(PRODUCER_NAME_1)
        producer2 = create_producer(PRODUCER_NAME_2)

        send_messages_to_topics(producer1, TOPICS_1, PRODUCER_NAME_1)
        send_messages_to_topics(producer2, TOPICS_2, PRODUCER_NAME_2)

    except Exception as e:
        logger.exception(f"Error during production: {e}")
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
