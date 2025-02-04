import os
import json
import time
import random
from kafka import KafkaProducer

# Get Kafka configuration from environment variables
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'default_input_topic')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
SLEEP_SECONDS = float(os.getenv('PRODUCER_SLEEP_SECONDS', '1.0'))


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=json_serializer,
            security_protocol='PLAINTEXT'
        )
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None


def produce_messages():
    producer = create_producer()

    if not producer:
        print("Failed to create Kafka producer. Exiting.")
        return

    try:
        while True:
            data = {
                "value": random.randint(0, 20),
                "timestamp": time.time()
            }

            producer.send(KAFKA_TOPIC, data)
            producer.flush()

            print(f"Sent message to topic {KAFKA_TOPIC}: {data}")

            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    produce_messages()