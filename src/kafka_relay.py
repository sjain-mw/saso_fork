from confluent_kafka import Consumer, Producer
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='kafka_benchmark.log',
    filemode='a'
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPIC = "bboxes"
DESTINATION_TOPIC = "exit-17"
GROUP_ID = "kafka_relay_group"

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'latest'
}

KAFKA_PRODUCER_CONF = {
    'bootstrap.servers': 'localhost:9094',
    'client.id': 'kafka_relay',
    'acks': '1',                   # Only leader acknowledgment (faster than 'all')
    'batch.size': 65536,            # 64 KB (allows batching multiple messages)
    'linger.ms': 2,                 # Small delay (2ms) to allow efficient batching
    'queue.buffering.max.messages': 100000,  # Handle message bursts
    'queue.buffering.max.kbytes': 1048576,   # 1 GB buffer for burst loads
    'message.max.bytes': 1048576,            # Allow up to 1 MB per message
}

# Create Kafka consumer and producer
consumer = Consumer(consumer_conf)
producer = Producer(KAFKA_PRODUCER_CONF)

# Subscribe to the source topic
consumer.subscribe([SOURCE_TOPIC])

# Callback function for async produce
def delivery_report(err, msg):
    produce_end_time = time.time()
    if err:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Produced message in {(produce_end_time - msg.timestamp()[1] / 1000):.2f} ms to {msg.topic()} [{msg.partition()}]")

def process_message(msg):
    if msg is None:
        return
    if msg.error():
        logging.error(f"Consumer error: {msg.error()}")
        return
    
    consume_start_time = time.time()
    message_value = msg.value().decode('utf-8')
    consume_end_time = time.time()
    consume_time = (consume_end_time - consume_start_time) * 1000  # Convert to ms
    logging.info(f"Consumed message in {consume_time:.2f} ms: {message_value}")
    
    # Asynchronously produce the message
    produce_start_time = time.time()
    producer.produce(DESTINATION_TOPIC, key=msg.key(), value=msg.value(), callback=delivery_report)
    
    # Allow Kafka to process pending events
    producer.poll(0)

logging.info(f"Listening for messages on topic: {SOURCE_TOPIC}")
try:
    while True:
        msg = consumer.poll(1.0)  # Timeout in seconds
        if msg:
            process_message(msg)
        producer.poll(0)  # Ensure delivery callbacks are handled
except KeyboardInterrupt:
    logging.info("Shutting down...")
finally:
    consumer.close()
    producer.flush()  # Ensure all messages are delivered before exit
