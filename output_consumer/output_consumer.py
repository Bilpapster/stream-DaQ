import os
import json
import csv
import time
from kafka import KafkaConsumer

# Get environment variables
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'output_topic')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
SLIDE_DURATION_STR = os.getenv('SLIDE_DURATION', '10 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()
NUM_CORES = os.getenv('SPARK_NUM_CORES', '20')


def get_output_file_name(window_type: str, window_duration: str, slide_duration: str | None, gap_duration: str | None, num_cores: str | int) -> str:
    window_type = str(window_type).lower()
    match window_type:
        case "tumbling":
            return f"daq_{window_type}_{window_duration}_{num_cores} cores.csv"
        case "sliding":
            return f"daq_{window_type}_{window_duration}_{slide_duration}_{num_cores} cores.csv"
        case "session":
            return f"daq_{window_type}_{gap_duration}_{num_cores} cores.csv"
        case _:
            return f"daq_{window_type}_{window_duration}_{slide_duration}_{gap_duration}_{num_cores} cores.csv"

CSV_OUTPUT_PATH = "data/" + get_output_file_name(WINDOW_TYPE, WINDOW_DURATION_STR, SLIDE_DURATION_STR, GAP_DURATION_STR, NUM_CORES)  # Path inside the container
print(f"Starting output consumer. Listening to topic: {OUTPUT_TOPIC}")
print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Output CSV path: {CSV_OUTPUT_PATH}")


def main():
    # Create Kafka consumer
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=12*60*1000  # Adjust as needed
    )

    fieldnames = None
    csv_file = open(CSV_OUTPUT_PATH, 'w', newline='')
    csv_writer = None

    try:
        for message in consumer:
            message_value = message.value  # Dictionary of the message value

            # Get the message timestamp (when Kafka broker received the message)
            timestamp = message.timestamp  # In milliseconds

            # Add the timestamp to the message
            message_value['timestamp'] = timestamp

            # Initialize CSV writer on the first message
            if fieldnames is None:
                fieldnames = list(message_value.keys())
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()  # Write header

            # Write message to CSV
            csv_writer.writerow(message_value)
            csv_file.flush()  # Ensure data is written to disk

    except KeyboardInterrupt:
        print("Stopping output consumer...")
    finally:
        print('Reached the finally branch')
        csv_file.close()
        consumer.close()

if __name__ == "__main__":
    main()