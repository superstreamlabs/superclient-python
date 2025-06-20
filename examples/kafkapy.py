"""
Kafka Producer using kafka-python library
This is the most popular pure Python client
"""
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from json_generator import generate_random_json

def create_producer(client_id):
    """Create and configure Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        client_id=client_id,
        compression_type=None,  # No compression
        batch_size=150,  # Batch size in bytes
        linger_ms=10,  # Linger time in milliseconds
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

def send_messages_to_topics(producer, topics, producer_name, num_messages=50):
    """Send random JSON messages to specified Kafka topics"""
    
    successful = 0
    failed = 0
    
    for i in range(num_messages):
        try:
            # Generate random JSON of at least 1KB
            message = generate_random_json(min_size_kb=1)
            message['message_number'] = i + 1
            message['producer'] = producer_name
            
            # Send message to each topic
            for topic in topics:
                future = producer.send(topic, value=message)
                # Wait for send to complete (optional, for confirmation)
                record_metadata = future.get(timeout=10)
            
            successful += 1
            
        except KafkaError as e:
            failed += 1
            print(f"Failed to send message {i+1}: {e}")
        
        # Small delay between messages (optional)
        time.sleep(0.01)
    
    # Flush remaining messages
    producer.flush()
    print(f"\n{producer_name} Summary: {successful} successful, {failed} failed")

def main():
    producer1 = None
    producer2 = None
    try:
        # Create two separate producers
        producer1 = create_producer('kafka-python-producer-1')
        producer2 = create_producer('kafka-python-producer-2')
        
        # First producer sends to test-topic and test-topic-1
        topics1 = ['test-topic', 'test-topic-1']
        send_messages_to_topics(producer1, topics1, 'kafka-python-producer-1')
        
        # Second producer sends to test-topic-2 and test-topic-3
        topics2 = ['test-topic-2', 'test-topic-3']
        send_messages_to_topics(producer2, topics2, 'kafka-python-producer-2')
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if producer1:
            producer1.close()
            print("Producer 1 closed")
        if producer2:
            producer2.close()
            print("Producer 2 closed")
    
    # Sleep for 10 minutes at the end
    print("Sleeping for 10 minutes...")
    time.sleep(600)
    print("Sleep completed")

if __name__ == "__main__":
    main()