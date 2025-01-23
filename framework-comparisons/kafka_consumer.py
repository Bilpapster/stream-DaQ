from configurations import get_kafka_topic, get_chance_of_missing_value, get_number_of_elements_to_send
from kafka import KafkaConsumer
import json
import sys

consumer = KafkaConsumer(
    get_kafka_topic(),
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

for message in consumer:
    print(message.value)

