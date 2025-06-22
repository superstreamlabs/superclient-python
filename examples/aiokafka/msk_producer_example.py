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

examples_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(examples_path)
from json_generator import generate_random_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("msk_producer")

# --- Cluster Configuration ---
BOOTSTRAP_SERVERS = "b-1-public.superstreamstg.halbm0.c1.kafka.eu-central-1.amazonaws.com:9198,b-2-public.superstreamstg.halbm0.c1.kafka.eu-central-1.amazonaws.com:9198"
TOPIC_NAME = "example-topic"


class MSKIAMTokenProvider(AbstractTokenProvider):
    """
    Custom aiokafka TokenProvider for AWS MSK IAM authentication.
    It automatically discovers the AWS region from the environment.
    """
    def __init__(self):
        self._botocore_session = get_session()
        
        self.region = self._botocore_session.get_config_variable('region')
        if not self.region:
            raise ValueError(
                "AWS Region not found. Please configure it via the AWS_REGION "
                "environment variable or in your ~/.aws/config file."
            )
        log.info(f"Initialized MSK IAM Token Provider for auto-detected region: {self.region}")

    async def token(self) -> str:
        """
        Generates and returns the authentication token using the discovered region.
        """
        try:
            token, _ = MSKAuthTokenProvider.generate_auth_token(
                self.region, self._botocore_session
            )
            log.info("Successfully generated MSK IAM auth token.")
            return token
        except Exception:
            log.exception("Failed to generate MSK IAM auth token.")
            raise


async def produce_messages(num_messages=100):
    """
    Connects to MSK and sends a specified number of random JSON messages.
    """
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    token_provider = MSKIAMTokenProvider()

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=token_provider,
        ssl_context=context,
        client_id="my-iam-generator-producer",
        request_timeout_ms=10000,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        log.info("Starting Kafka producer...")
        await producer.start()
        log.info("Producer started successfully.")
        
        log.info(f"Sending {num_messages} messages to topic '{TOPIC_NAME}'...")
        for i in range(num_messages):
            message = generate_random_json()
            message['message_number'] = i + 1
            
            await producer.send_and_wait(TOPIC_NAME, value=message)
            log.info(f"Sent message #{i + 1} with ID {message['message_number']}")
            await asyncio.sleep(0.1)

        log.info(f"Successfully sent {num_messages} messages.")

    except Exception:
        log.exception("An error occurred while running the producer.")
        log.error(
            "CRITICAL: If you see connection or authentication errors, "
            "verify the IAM policy for your user/role."
        )
    finally:
        if producer:
            log.info("Stopping producer...")
            await producer.stop()
            log.info("Producer stopped.")


if __name__ == "__main__":
    asyncio.run(produce_messages())