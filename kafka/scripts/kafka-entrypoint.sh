#!/bin/bash

echo "Creating the following topics ..."
echo "INPUT_TOPIC: $INPUT_TOPIC"
echo "OUTPUT_TOPIC: $OUTPUT_TOPIC"

# Create topics for input and output, based on environment variables
kafka-topics --bootstrap-server kafka:29092 --create --topic $INPUT_TOPIC  --partitions $SPARK_NUM_CORES --replication-factor 1 --config retention.ms=10000 # 10-second retention
kafka-topics --bootstrap-server kafka:29092 --create --topic $OUTPUT_TOPIC --partitions $SPARK_NUM_CORES --replication-factor 1 --config retention.ms=5000  # 5-second retention

