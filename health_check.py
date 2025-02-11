from kafka import KafkaConsumer
from json import loads
import time
import subprocess



def execute_kafka_shell_script():
    # Execute the shell script
    script_path = './restart-kafkaservice.sh'  # Make sure the script is in the current directory or provide the full path
    subprocess.run(['bash', script_path], check=True)

def execute_sasocv_shell_script():
    # Execute the shell script
    script_path = './restart-sasocvservice.sh'  # Make sure the script is in the current directory or provide the full path
    subprocess.run(['bash', script_path], check=True)

try:
    # Configure the Kafka consumer
    consumer = KafkaConsumer(
        'health-check',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='alert-group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
except Exception as e:
    print(f"Error configuring Kafka consumer: {e}")
    execute_kafka_shell_script()
    time.sleep(60)

# Initialize a variable to track the time of the last received message
last_received_time = time.time()

try:
    print("Listening for messages. Executing script if no messages in 10 seconds.")
    while True:

        try:
            message = consumer.poll(timeout_ms=1000)
        except Exception as e:
            print(f"Error polling Kafka consumer: {e}")
            print("Kafka broker unavailable. Executing script.")
            execute_kafka_shell_script()
            time.sleep(60)
            last_received_time = time.time()
            continue

        if message:
            for topic, records in message.items():
                for record in records:
                    last_received_time = time.time()
                    print(f"Received message: {record.value}")
            
        elif time.time() - last_received_time > 10:
            print("Alert: No messages received for 10 seconds, executing script.")
            execute_sasocv_shell_script()
            time.sleep(60)
            # Reset the timer
            last_received_time = time.time()

except KeyboardInterrupt:
    print("Stopped by user.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Clean up: close the consumer
    consumer.close()
    print("Consumer closed.")

