import os
import csv
import json
import time
from kafka import KafkaProducer

# Get Kafka configuration from environment variables
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
REDDIT_CSV_FILE = os.getenv('REDDIT_CSV_FILE', 'datasets/reddit-comments-may-2015-small.csv')

FIELD_CASTS = {
    'created_utc': int,
    'score_hidden': int,
    'glided': int,
    'ups': int,
    'downs': int,
    'archived': int,
    'score': int,
    'retrieved_on': int,
    'edited': int,
    'controversiality': int
}


def json_serializer(data):
    """Converts a Python dictionary into a JSON-encoded bytes object."""
    return json.dumps(data).encode('utf-8')


def create_producer():
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=json_serializer,
            security_protocol='PLAINTEXT'
        )
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None


def preprocess_and_type_cast_fields(row: dict, field_casts: dict) -> dict:
    """
    For each field in 'field_casts', apply the cast function if the field exists in 'row'.
    """
    for field_name, cast_fn in field_casts.items():
        if field_name in row and row[field_name] is not None:
            try:
                row[field_name] = cast_fn(row[field_name])
            except ValueError:
                # Handle errors e.g., row[field_name] was not parseable
                print(f"Failed to cast field '{field_name}' with value '{row[field_name]}'")
                row[field_name] = None
    row["id_"] = row["id"]
    row.pop("id", None) # remove the "id" key to only keep the "id_" one
    return row


def produce_records_from_csv():
    """Reads records from the huge Reddit-data CSV file line by line and produces them to Kafka."""
    producer = create_producer()
    if not producer:
        print("Failed to create Kafka producer. Exiting.")
        return

    try:
        with open(REDDIT_CSV_FILE, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            count = 0
            start_time = time.time()

            for row in reader:

                row = preprocess_and_type_cast_fields(row, FIELD_CASTS)

                # Send the record to Kafka
                producer.send(KAFKA_TOPIC, row)
                count += 1

                # Optionally, flush periodically
                if count % 10000 == 0:
                    producer.flush()
                    print(f"{count} records sent so far...")

            # Artificially add an "end" event to force push computation of the window
            # for row in reader:
            #     preprocess_and_type_cast_fields(row, FIELD_CASTS)
            #     row["created_utc"] = int(time.time())
            #     producer.send(KAFKA_TOPIC, row)
            #     break

            # Final flush after reading all lines
            producer.flush()
            elapsed = time.time() - start_time
            print(f"Done sending {count} records. Elapsed time: {elapsed:.2f} seconds")

    except FileNotFoundError:
        print(f"CSV file '{REDDIT_CSV_FILE}' not found.")
    except Exception as e:
        print(f"Error while reading CSV or producing messages to Kafka: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    print(f"Starting stream: {time.time()} UNIX timestamp")
    produce_records_from_csv()
    print(f"Finished stream: {time.time()} UNIX timestamp")
