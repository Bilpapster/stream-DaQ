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
    value = random.randint(0, 100)
    coin_flip_for_missing_value = random.uniform(0, 1) <= get_chance_of_missing_value()
    if coin_flip_for_missing_value:
        value = ''  # missing value with some probability, specified in configurations.py

    # Send JSON data
    producer.send(get_kafka_topic(), {'value': value})
