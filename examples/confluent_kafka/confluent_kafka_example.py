# # import os
# # import sys
# # import logging
# # import signal
# # from confluent_kafka import Producer

# # # === Logger Setup ===
# # logging.basicConfig(level=logging.INFO)
# # logger = logging.getLogger("ConfluentProducerExample")

# # # === Configuration Constants ===
# # DEFAULT_BOOTSTRAP_SERVERS = "pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092"
# # CONFLUENT_USERNAME = "DQMG57UH2JKICKDK"
# # CONFLUENT_PASSWORD = "Ntt+6A5vNyunC/VontJnQsZ4Ydfs6XM1S6rcydV8BYbKTgXzcArfmj6jkcCvozgC"

# # SECURITY_PROTOCOL = "SASL_SSL"
# # SASL_MECHANISM = "PLAIN"
# # CLIENT_ID = "superstream-example-producer"
# # COMPRESSION_TYPE = "gzip"
# # BATCH_NUM_MESSAGES = 16384
# # LINGER_MS = 500

# # TOPIC_NAME = "example-topic"
# # MESSAGE_KEY = "test-key"
# # MESSAGE_VALUE = "Hello, Superstream - test shoham!"

# # # === Graceful Shutdown Handler ===
# # def handle_exit(signum, frame):
# #     logger.warning("Interrupt received (signal %d). Shutting down...", signum)
# #     sys.exit(0)

# # signal.signal(signal.SIGINT, handle_exit)
# # signal.signal(signal.SIGTERM, handle_exit)

# # # === Delivery callback ===
# # def delivery_report(err, msg):
# #     if err is not None:
# #         logger.error("Delivery failed: %s", err)
# #     else:
# #         logger.info("Delivered message to %s [%d]", msg.topic(), msg.partition())

# # def main():
# #     bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)

# #     config = {
# #         "bootstrap.servers": bootstrap_servers,
# #         "security.protocol": SECURITY_PROTOCOL,
# #         "sasl.mechanism": SASL_MECHANISM,
# #         "sasl.username": CONFLUENT_USERNAME,
# #         "sasl.password": CONFLUENT_PASSWORD,
# #         "client.id": CLIENT_ID,
# #         "compression.type": COMPRESSION_TYPE,
# #         "batch.num.messages": BATCH_NUM_MESSAGES,
# #         "linger.ms": LINGER_MS 
# #     }

# #     logger.info("Creating producer with bootstrap servers: %s", bootstrap_servers)
# #     for k, v in config.items():
# #         redacted = v if "password" not in k else "***REDACTED***"
# #         logger.info("  %s = %s", k, redacted)

# #     producer = Producer(config)

# #     try:
# #         logger.info("Sending message to topic %s: key=%s, value=%s", TOPIC_NAME, MESSAGE_KEY, MESSAGE_VALUE)
# #         producer.produce(
# #             topic=TOPIC_NAME,
# #             key=MESSAGE_KEY,
# #             value=MESSAGE_VALUE,
# #             callback=delivery_report
# #         )

# #         logger.info("Flushing pending messages (max 15 seconds)...")
# #         producer.flush(15)
# #         logger.info("Message flushed and sent successfully")

# #     except KeyboardInterrupt:
# #         logger.warning("Interrupted by user. Attempting quick flush...")
# #         producer.flush(2)
# #     except Exception as e:
# #         logger.error("Unexpected error during production", exc_info=e)
# #     finally:
# #         logger.info("Exiting producer cleanly")

# # if __name__ == "__main__":
# #     main()
# import json
# import time
# import sys
# import os
# from confluent_kafka import Producer
# # from json_generator import generate_random_json

# # Ensure `examples/` is in the import path
# # examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# # sys.path.append(examples_path)

# examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# sys.path.append(examples_path)

# from json_generator import generate_random_json

# # Confluent Cloud connection config
# BOOTSTRAP_SERVERS = 'pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092'
# SASL_USERNAME = 'DQMG57UH2JKICKDK'
# SASL_PASSWORD = 'Ntt+6A5vNyunC/VontJnQsZ4Ydfs6XM1S6rcydV8BYbKTgXzcArfmj6jkcCvozgC'

# PRODUCER_NAME_1 = 'confluent-python-producer-1'
# PRODUCER_NAME_2 = 'confluent-python-producer-2'
# TOPICS_1 = ['example-topic']
# TOPICS_2 = ['example-topic']

# def create_producer(client_id: str) -> Producer:
#     """Create and configure Confluent Kafka producer"""
#     config = {
#         'bootstrap.servers': BOOTSTRAP_SERVERS,
#         'security.protocol': 'SASL_SSL',
#         'sasl.mechanisms': 'PLAIN',
#         'sasl.username': SASL_USERNAME,
#         'sasl.password': SASL_PASSWORD,
#         'client.id': client_id,
#         'linger.ms': 10,
#         'batch.num.messages': 1000
#     }
#     return Producer(config)

# def delivery_report(err, msg):
#     """Callback called once for each message delivery attempt"""
#     if err is not None:
#         print(f'❌ Delivery failed for record: {err}')
#     else:
#         print(f'✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# def send_messages_to_topics(producer: Producer, topics, producer_name: str, num_messages=50):
#     """Send random JSON messages to specified Kafka topics"""
#     successful = 0
#     failed = 0

#     for i in range(num_messages):
#         try:
#             message = generate_random_json(min_size_kb=1)
#             message['message_number'] = i + 1
#             message['producer'] = producer_name
#             encoded = json.dumps(message).encode('utf-8')

#             for topic in topics:
#                 producer.produce(topic=topic, value=encoded, callback=delivery_report)

#             successful += 1
#         except Exception as e:
#             failed += 1
#             print(f"❌ Failed to produce message {i + 1}: {e}")

#         producer.poll(0)  # Trigger delivery callbacks
#         time.sleep(0.01)

#     producer.flush()
#     print(f"\n{producer_name} Summary: {successful} successful, {failed} failed")

# def main():
#     producer1 = create_producer(PRODUCER_NAME_1)
#     producer2 = create_producer(PRODUCER_NAME_2)

#     try:
#         send_messages_to_topics(producer1, TOPICS_1, PRODUCER_NAME_1)
#         send_messages_to_topics(producer2, TOPICS_2, PRODUCER_NAME_2)
#     except Exception as e:
#         print(f"🚨 Error: {e}")
#     finally:
#         print("💤 Sleeping for 10 minutes...")
#         time.sleep(600)
#         print("✅ Sleep completed")

# if __name__ == "__main__":
#     main()

import json
import time
import sys
import os
from confluent_kafka import Producer

# Adjust path to import from examples/json_generator.py
examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)

from json_generator import generate_random_json

# Confluent Cloud connection config
PRODUCER_NAME_1 = 'confluent-kafka-producer-1'
PRODUCER_NAME_2 = 'confluent-kafka-producer-2'
TOPICS_1 = ['example-topic']
TOPICS_2 = ['example-topic']

BOOTSTRAP_SERVERS = 'pkc-7xoy1.eu-central-1.aws.confluent.cloud:9092'
SASL_USERNAME = 'DQMG57UH2JKICKDK'
SASL_PASSWORD = 'Ntt+6A5vNyunC/VontJnQsZ4Ydfs6XM1S6rcydV8BYbKTgXzcArfmj6jkcCvozgC'

def create_producer(client_id):
    """Create and configure Kafka producer for Confluent Cloud using SASL_SSL"""
    return Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': SASL_USERNAME,
        'sasl.password': SASL_PASSWORD,
        'client.id': client_id,
        'linger.ms': 10,
        'batch.num.messages': 1000
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
