import json
from kafka import KafkaProducer
import random
import time
from config.configurations import (
    kafka_topic, kafka_server,
    number_of_elements_to_send,
    chance_of_missing_value,
    sleep_seconds_between_messages
)
from utils import is_out_of_range

api_version = (0, 10, 2)

# Create a producer with JSON serializer
producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    security_protocol='PLAINTEXT',
    api_version=api_version,
)

total_missing_values = total_out_of_range = 0
for _ in range(number_of_elements_to_send):
    value = random.randint(0, 100)
    coin_flip_for_missing_value = random.uniform(0, 1) <= chance_of_missing_value
    if coin_flip_for_missing_value:
        value = None  # missing value with some probability, specified in configurations.py
        total_missing_values += 1
    if is_out_of_range(value):
        total_out_of_range += 1

    # Send JSON data
    producer.send(kafka_topic, (json.dumps({'value': value})).encode('utf-8'))
    time.sleep(sleep_seconds_between_messages)

print(f"Total missing values: {total_missing_values}")
print(f"Total out of range  : {total_out_of_range}")
