from confluent_kafka import Consumer, KafkaError
from config import PLATFORM_PRODUCER_TOPIC, TEST_PLATFORM_PRODUCER_TOPIC
# Kafka configuration
config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'python-consumer-group',    # Consumer group ID
    'auto.offset.reset': 'latest'         # Where to start reading if no offset exists
}

# Topic to subscribe to
topic = TEST_PLATFORM_PRODUCER_TOPIC  # Replace with your topic name

def consume_messages():
    # Create a Kafka consumer
    consumer = Consumer(config)
    consumer.subscribe([topic])

    try:
        print(f"Subscribed to topic: {topic}")
        while True:
            # Poll for a message
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # No message, continue polling

            if msg.error():
                # Handle errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("End of partition reached")
                else:
                    print(f"Error: {msg.error()}")
                continue

            # Process the message
            # v = msg.value().decode('utf-8')
            # if "1c52645e-caa2-4d4d-8a37-e6a22350bf0b" in v:
                
            print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("Consumer interrupted")
    finally:
        # Close the consumer to commit offsets and release resources
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    consume_messages()



def consume_messages(self):
        # Create a Kafka consumer
        consumer = Consumer(config)
        consumer.subscribe(["saso-inference"])

        try:
            print(f"Subscribed to topic: saso-inference")
            while True:
                # Poll for a message
                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue  # No message, continue polling

                if msg.error():
                    # Handle errors
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("End of partition reached")
                    else:
                        print(f"Error: {msg.error()}")
                    continue

                # Process the message
                print(f"Received message: {msg.value().decode('utf-8')}")
        except KeyboardInterrupt:
            print("Consumer interrupted")
        finally:
            # Close the consumer to commit offsets and release resources
            consumer.close()
            print("Consumer closed")