import json
from kafka import KafkaProducer
import random
import time
from config.configurations import (
    kafka_topic, kafka_server,
    number_of_elements_to_send, chance_of_missing_value,
    max_stream_value, min_stream_value,
    sleep_seconds_between_messages
)
from utils import is_out_of_range

api_version = (0, 10, 2)

producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    security_protocol='PLAINTEXT',
    api_version=api_version,
)

total_missing_values = total_out_of_range = 0 # counters to keep track of the missing and out of range elements sent
for _ in range(number_of_elements_to_send):
    value = random.randint(min_stream_value, max_stream_value) # generate a random number in the configured range
    coin_flip_for_missing_value = random.uniform(0, 1) <= chance_of_missing_value # determine whether the value will be 'missing'
    if coin_flip_for_missing_value:
        value = None
        total_missing_values += 1
    if is_out_of_range(value):
        total_out_of_range += 1

    # Send JSON data to the topic. Data will then be read by the streaming frameworks.
    producer.send(kafka_topic, (json.dumps({'value': value})).encode('utf-8'))
    # Wait a tiny amount of time to ensure no messages are lost. It can be altered via config/configurations.yaml.
    time.sleep(sleep_seconds_between_messages)

# Inform user about the missing and out of range values sent
print(f"Finished sending {number_of_elements_to_send} values")
print(f"Total missing values: {total_missing_values} ({total_missing_values/number_of_elements_to_send*100:.2f}%)")
print(f"Total out of range  : {total_out_of_range} ({total_out_of_range/number_of_elements_to_send*100:.2f}%)")
