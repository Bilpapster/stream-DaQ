from kafka import KafkaConsumer
from configurations import kafka_topic

api_version = (0, 10, 2)

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=['localhost:9092'],
    security_protocol='PLAINTEXT',
    api_version=api_version,
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

for message in consumer:
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                         message.offset, message.key,
                                         message.value))
