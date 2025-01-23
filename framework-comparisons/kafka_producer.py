from configurations import get_kafka_topic, get_chance_of_missing_value, get_number_of_elements_to_send
import json
from kafka import KafkaProducer
import random

# Create a producer with JSON serializer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for _ in range(get_number_of_elements_to_send()):
    coin_flip = random.uniform(0, 1)
    if coin_flip < get_chance_of_missing_value():
        producer.send(get_kafka_topic(), {'value': ''})
        print('sent a missing value')
        continue
    # Sending JSON data
    print('sent a normal value')
    producer.send(get_kafka_topic(), {'value': random.randint(0, 100)})
