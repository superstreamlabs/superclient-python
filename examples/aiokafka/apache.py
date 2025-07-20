"""
Kafka Producer using aiokafka library
This is an asyncio-based client, great for async Python applications
"""
import json
import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from json_generator import generate_random_json

async def create_producer(client_id):
    """Create and configure Kafka producer"""
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        client_id=client_id,
        compression_type=None,  # No compression
        max_batch_size=150,  # Batch size in bytes
        linger_ms=10,  # Linger time
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    await producer.start()
    return producer

async def send_messages_to_topics(producer, topics, producer_name, num_messages=5):
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
                result = await producer.send(topic, message)
            
            successful += 1
            
        except KafkaError as e:
            failed += 1
            print(f"Failed to send message {i+1}: {e}")
        
        # Small delay between messages (optional)
        await asyncio.sleep(0.01)
    
    print(f"\n{producer_name} Summary: {successful} successful, {failed} failed")

async def main():
    producer1 = None
    producer2 = None
    try:
        # Create two separate producers
        producer1 = await create_producer('aiokafka-producer-1')
        # producer2 = await create_producer('aiokafka-producer-2')
        
        # First producer sends to test-topic and test-topic-1
        topics1 = ['test-topic', 'test-topic-1']
        await send_messages_to_topics(producer1, topics1, 'aiokafka-producer-1')
        
        # Second producer sends to test-topic-2 and test-topic-3
        topics2 = ['test-topic-2', 'test-topic-3']
        # await send_messages_to_topics(producer2, topics2, 'aiokafka-producer-2')
        # Sleep for 10 minutes at the end
        print("Sleeping for 10 minutes...")
        await asyncio.sleep(600)
        print("Sleep completed")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if producer1:
            await producer1.stop()
            print("Producer 1 closed")
        # if producer2:
        #     await producer2.stop()
        #     print("Producer 2 closed")
    

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())