import asyncio
import ssl
import logging
import os
import json
import sys
from aiokafka.abc import AbstractTokenProvider
from aiokafka import AIOKafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.session import get_session

# --- Configuration Section ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("msk_iam_producer")

# === MSK Cluster Config ===
BOOTSTRAP_SERVERS = "b-1-public.your-msk-cluster.amazonaws.com:9198,b-2-public.your-msk-cluster.amazonaws.com:9198"
TOPICS = ["example-topic"]

# === AWS IAM Config ===
AWS_ACCESS_KEY_ID = None
AWS_SECRET_ACCESS_KEY = None
AWS_SESSION_TOKEN = None

# === Producer Config ===
CLIENT_ID = "iam-producer-instance-1"
PRODUCER_NAME = "iam-producer-instance-1"
MAX_BATCH_SIZE = 16384
LINGER_MS = 100

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json


class MSKIAMTokenProvider(AbstractTokenProvider):
    """Custom TokenProvider that uses botocore to handle credential discovery."""
    def __init__(self, access_key=None, secret_key=None, session_token=None):
        self._botocore_session = get_session()
        if access_key and secret_key:
            log.info("Overwriting default credentials with keys provided directly in the script.")
            self._botocore_session.set_credentials(access_key, secret_key, session_token)
        else:
            log.info("Using botocore's default credential discovery.")
        
        self.region = self._botocore_session.get_config_variable('region')
        if not self.region:
            raise ValueError("AWS Region not found.")
        log.info(f"Token provider initialized for region: {self.region}")

    async def token(self) -> str:
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region, self._botocore_session)
        return token


async def create_producer(client_id, **aws_creds):
    """Creates and starts an MSK IAM-compatible AIOKafkaProducer."""
    log.info(f"Creating async producer with client.id: {client_id}")
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    token_provider = MSKIAMTokenProvider(**aws_creds)
    
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id=client_id,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=token_provider,
        ssl_context=context,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_batch_size=MAX_BATCH_SIZE,
        linger_ms=LINGER_MS,
    )
    await producer.start()
    return producer


async def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Generates and sends messages using the provided producer instance."""
    successful = 0
    failed = 0
    log.info(f"Sending {num_messages} messages from logical producer '{producer_name}' to topics: {topics}")
    
    for i in range(num_messages):
        try:
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            key = f"msg-{i + 1}"
            for topic in topics:
                await producer.send(topic, key=key, value=message)
            log.info(f"Queued message #{i + 1} with key '{key}' for all topics.")
            successful += 1
        except Exception as e:
            failed += 1
            log.error(f"Failed to queue message #{i + 1}: {e}")
    
    log.info("Flushing final messages...")
    await producer.flush()
    log.info(f"--- '{producer_name}' Summary: {successful} queued, {failed} failed ---")


async def main():
    """The main entry point for the script."""
    producer = None
    try:
        producer = await create_producer(
            CLIENT_ID,
            access_key=AWS_ACCESS_KEY_ID,
            secret_key=AWS_SECRET_ACCESS_KEY,
            session_token=AWS_SESSION_TOKEN
        )
        await send_messages_to_topics(producer, TOPICS, PRODUCER_NAME)
    except Exception as e:
        log.exception(f"A critical error occurred in main: {e}")
    finally:
        if producer:
            log.info("Attempting to stop the producer...")
            try:
                await producer.flush()
                await producer.stop()
                log.info("Producer stopped successfully.")
            except Exception as e:
                log.exception(f"An error occurred while stopping the producer: {e}")


if __name__ == "__main__":
    asyncio.run(main())