import logging
import os
import json
import sys
import time
from confluent_kafka import Producer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.session import get_session

# --- Configuration Section ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("msk_iam_producer_confluent")

# === MSK Cluster Config ===
BOOTSTRAP_SERVERS = "b-1-public.superstreamstg.halbm0.c1.kafka.eu-central-1.amazonaws.com:9198,b-2-public.superstreamstg.halbm0.c1.kafka.eu-central-1.amazonaws.com:9198"
TOPICS = ["example-topic"]

# === AWS IAM Config ===
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
AWS_SESSION_TOKEN = None

# === Producer Config ===
CLIENT_ID = "iam-producer-confluent-1"
PRODUCER_NAME = "iam-producer-confluent-1"
BATCH_SIZE = 16384
LINGER_MS = 100

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json


def create_producer(client_id, **aws_creds):
    """Creates and returns a confluent-kafka Producer instance."""
    log.info(f"Creating producer with client.id: {client_id}")

    botocore_session = get_session()
    if aws_creds.get('access_key') and aws_creds.get('secret_key'):
        log.info("Using credentials provided directly in the script.")
        botocore_session.set_credentials(
            aws_creds['access_key'], aws_creds['secret_key'], aws_creds.get('session_token')
        )
    else:
        log.info("Using botocore's default credential discovery.")
    
    region = botocore_session.get_config_variable('region')
    if not region:
        raise ValueError("AWS Region not found.")
    log.info(f"Token provider initialized for region: {region}")

    def oauth_cb(oauth_config):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region, botocore_session)
        return token, time.time() + (expiry_ms / 1000.0)

    # --- Producer Configuration Dictionary ---
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'client.id': client_id,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': oauth_cb,
        # Batching options for performance
        'batch.size': BATCH_SIZE,
        'linger.ms': LINGER_MS,
    }

    return Producer(config)


def delivery_report(err, msg):
    """Callback triggered by produce() to report message delivery status."""
    if err is not None:
        log.error(f"Message delivery failed: {err}")
    else:
        log.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Generates and sends messages using the provided producer instance."""
    log.info(f"Sending {num_messages} messages from logical producer '{producer_name}' to topics: {topics}")
    
    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            key = f"msg-{i + 1}"
            value = json.dumps(message).encode('utf-8')

            for topic in topics:
                producer.produce(topic, key=key, value=value, callback=delivery_report)
            
            producer.poll(0)
            
        except BufferError:
            log.warning("Local queue is full. Waiting...")
            producer.poll(1) 
        except Exception as e:
            log.error(f"Failed to produce message #{i+1}: {e}")
    
    log.info("Flushing final messages...")
    remaining = producer.flush()
    if remaining > 0:
        log.info(f"Waited for {remaining} outstanding messages to be delivered.")


def main():
    """The main entry point for the script."""
    producer = None
    try:
        producer = create_producer(
            CLIENT_ID,
            access_key=AWS_ACCESS_KEY_ID,
            secret_key=AWS_SECRET_ACCESS_KEY,
            session_token=AWS_SESSION_TOKEN
        )
        send_messages_to_topics(producer, TOPICS, PRODUCER_NAME)
    except Exception as e:
        log.exception(f"A critical error occurred: {e}")
    finally:
        if producer:
            log.info("Producer flush finished.")


if __name__ == "__main__":
    main()