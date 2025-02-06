#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to get ready ..."
# cub command is part of Confluent's utilities to ensure Kafka is running before proceeding
cub kafka-ready -b localhost:9092 1 60

# Print environment variables for debugging
echo "INPUT_TOPIC: $INPUT_TOPIC"
echo "OUTPUT_TOPIC: $OUTPUT_TOPIC"

# Create topics for input and output
kafka-topics --bootstrap-server localhost:9092 --create --topic $INPUT_TOPIC  --partitions 1 --replication-factor 1 --config retention.ms=600000
kafka-topics --bootstrap-server localhost:9092 --create --topic $OUTPUT_TOPIC --partitions 1 --replication-factor 1 --config retention.ms=600000

exec /etc/confluent/docker/run
