import json
import time
import sys
import os
from confluent_kafka import Producer

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)

from json_generator import generate_random_json

# Confluent Cloud connection config
BOOTSTRAP_SERVERS = 'your-cluster-name.region.aws.confluent.cloud:9092'
SASL_USERNAME = 'your-confluent-api-key'
SASL_PASSWORD = 'your-confluent-api-secret'

# === Producer Config
BATCH_SIZE = 150
LINGER_MS = 10
PRODUCER_NAME_1 = 'confluent-kafka-producer-1'
PRODUCER_NAME_2 = 'confluent-kafka-producer-2'
TOPICS_1 = ['example-topic', 'test1']
TOPICS_2 = ['example-topic', 'test2']

def create_producer(client_id):
    """Create and configure Kafka producer for Confluent Cloud using SASL_SSL"""
    return Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': SASL_USERNAME,
        'sasl.password': SASL_PASSWORD,
        'client.id': client_id,
        'linger.ms': LINGER_MS,
        'batch.num.messages': BATCH_SIZE
    })

def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Send random JSON messages to specified Kafka topics"""
    successful = 0
    failed = 0

    def delivery_report(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            print(f"Failed to send message to {msg.topic()}: {err}")

    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            encoded = json.dumps(message).encode('utf-8')

            for topic in topics:
                producer.produce(topic=topic, value=encoded, callback=delivery_report)

            successful += 1
        except Exception as e:
            failed += 1
            print(f"Failed to send message {i + 1}: {e}")

        producer.poll(0)
        time.sleep(0.01)

    producer.flush()
    print(f"\n{producer_name} Summary: {successful} successful, {failed} failed")

def main():
    producer1 = producer2 = None
    try:
        producer1 = create_producer(PRODUCER_NAME_1)
        producer2 = create_producer(PRODUCER_NAME_2)

        send_messages_to_topics(producer1, TOPICS_1, PRODUCER_NAME_1)
        send_messages_to_topics(producer2, TOPICS_2, PRODUCER_NAME_2)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if producer1:
            print("Producer 1 closed")
        if producer2:
            print("Producer 2 closed")

    print("Sleeping for 10 minutes...")
    time.sleep(600)
    print("Sleep completed")

if __name__ == "__main__":
    main()
