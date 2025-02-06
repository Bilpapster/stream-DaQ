import os, json, time, random, string
from datetime import datetime
from kafka import KafkaProducer

# Get Kafka configuration from environment variables
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
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
                'productName': f"product{str(random.randint(0, 20))}",
                'id_': f'id{str(random.randint(0, 100))}',
                'description': ''.join(random.choices(string.ascii_uppercase + string.digits + ' -', k=random.randint(10, 200))),
                'priority': random.choice(["low", "normal", "medium", "high"]),
                'numViews': random.randint(1, 1000),
                'eventTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            }

            producer.send(KAFKA_TOPIC, data)
            # producer.flush()
            # print(f"Sent message to topic {KAFKA_TOPIC}: {data}")

            time.sleep(random.random() / random.randint(1, 10000000000000000))

    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    produce_messages()