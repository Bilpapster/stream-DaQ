import json
from kafka import KafkaProducer
import random
import time
from datetime import datetime, timedelta
from config.configurations import (
    kafka_topic, kafka_server,
    number_of_elements_to_send, chance_of_missing_value,
    max_stream_value, min_stream_value,
    sleep_seconds_between_messages,
    directory_base_name
)
from utils import standardize_timestamp_to_nanoseconds

def send_message(sleep_seconds: float):
    value = random.randint(min_stream_value, max_stream_value)  # generate a random number in the configured range
    coin_flip_for_missing_value = random.uniform(0,
                                                 1) <= chance_of_missing_value  # determine whether the value will be 'missing'
    if coin_flip_for_missing_value:
        value = None
    data = {
        'productName': f"product{str(random.randint(0, 20))}",
        'id': f'id{str(value)}', # for deequ
        # 'id_': f'id{str(value)}', # for pathway
        'description': f"This is some description{str(random.randint(0, 20))}",
        'priority': random.choice(["low", "normal", "medium", "high"]),
        'numViews': value,
        'eventTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], # for deequ
        # 'eventTime': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], # for Stream DaQ
        # 'eventTime': standardize_timestamp_to_nanoseconds(time.time()),  # for Stream DaQ
    }

    # Send JSON data to the topic. Data will then be read by the streaming frameworks.
    producer.send(kafka_topic, (json.dumps(data)).encode('utf-8'))
    # Wait a tiny amount of time to ensure no messages are lost. It can be altered via config/configurations.yaml.
    time.sleep(sleep_seconds)

api_version = (0, 10, 2)

producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    security_protocol='PLAINTEXT',
    api_version=api_version,
)

warmup_element_counts = [200]
element_counts_per_window = list(range(1, 12001, 100))
window_duration_sec = 20

# warmup windows that will not be counted
# for element_count in warmup_element_counts:
#
#     sleep_seconds = float(window_duration_sec / element_count+1)
#     for element in range(element_count):
#         send_message(sleep_seconds)

# actual windows that will be counted
for element_count in element_counts_per_window:
    sleep_seconds = float(window_duration_sec / element_count)
    for element in range(element_count):
        send_message(sleep_seconds)
