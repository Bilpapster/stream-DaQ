import json
from kafka import KafkaProducer
import random
import time
from configurations import (
    kafka_topic,
    number_of_elements_to_send,
    chance_of_missing_value,
    sleep_seconds_between_messages
)

api_version = (0, 10, 2)

# Create a producer with JSON serializer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    security_protocol='PLAINTEXT',
    api_version=api_version,
)

total_missing_values = 0
for _ in range(number_of_elements_to_send):
    value = random.randint(0, 100)
    coin_flip_for_missing_value = random.uniform(0, 1) <= chance_of_missing_value
    if coin_flip_for_missing_value:
        value = None  # missing value with some probability, specified in configurations.py
        total_missing_values += 1

    # Send JSON data
    producer.send(kafka_topic, (json.dumps({'value': value})).encode('utf-8'))
    time.sleep(sleep_seconds_between_messages)

print(f"Total missing values: {total_missing_values}")
