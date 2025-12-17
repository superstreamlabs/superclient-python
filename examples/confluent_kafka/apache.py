"""
Kafka Producer using confluent-kafka library
This is the Python wrapper around librdkafka (C library), offering high performance
"""
import json
import time
from confluent_kafka import Producer
from json_generator import generate_random_json

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')

def create_producer(client_id):
    """Create and configure Kafka producer"""
    config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': client_id,
        'compression.type': 'none',
        'batch.size': 150,  # Batch size in bytes
        'linger.ms': 10,  # Linger time
    }
    return Producer(config)

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
            
            # Serialize to JSON
            message_json = json.dumps(message)
            
            # Send message to each topic
            for topic in topics:
                producer.produce(
                    topic=topic,
                    value=message_json.encode('utf-8'),
                    callback=delivery_report
                )
                # Trigger any available delivery report callbacks
                producer.poll(0)
            
            successful += 1
            
        except Exception as e:
            failed += 1
            print(f"Failed to send message {i+1}: {e}")
        
        # Small delay between messages (optional)
        time.sleep(2)

    print(f"\n{producer_name} Summary: {successful} successful, {failed} failed")

def main():
    producer1 = None
    producer2 = None
    try:
        # Create two separate producers
        producer1 = create_producer('confluent-kafka-producer-1')
        # producer2 = create_producer('confluent-kafka-producer-2')
        
        # First producer sends to test-topic and test-topic-1
        topics1 = ['test-topic', 'test-topic-1']
        send_messages_to_topics(producer1, topics1, 'confluent-kafka-producer-1')
        
        # Second producer sends to test-topic-2 and test-topic-3
        topics2 = ['test-topic-2', 'test-topic-3']
        # send_messages_to_topics(producer2, topics2, 'confluent-kafka-producer-2')
        print("Sleeping...")
        time.sleep(30)
        print("Sleep completed")
    except Exception as e:
        print(f"Error: {e}")
    
    # Explicitly set producers to None to force garbage collection
    print("Setting producers to None to trigger Superstream cleanup...")
    producer1 = None
    # producer2 = None
    
    # Force garbage collection to trigger __del__ immediately
    import gc
    print("Forcing garbage collection...")
    gc.collect()
    print("Garbage collection completed")

if __name__ == "__main__":
    main()