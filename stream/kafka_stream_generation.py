import os, json, time, random, string
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Get configuration from environment variables
KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
MESSAGES_PER_WINDOW_LIST = os.getenv('MESSAGES_PER_WINDOW', '1000')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()

# print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
# print(f"KAFKA_SERVER: {KAFKA_SERVER}")
# print(f"WINDOW_DURATION_STR: {WINDOW_DURATION_STR}")
# print(f"MESSAGES_PER_WINDOW_LIST: {MESSAGES_PER_WINDOW_LIST}")
# print(f"WINDOW_TYPE: {WINDOW_TYPE}")
# print(f"GAP_DURATION_STR: {GAP_DURATION_STR}")

def standardize_timestamp_to_milliseconds(floating_point_unix_timestamp: float) -> int:
    return int(floating_point_unix_timestamp * 1e3)

def wait_until_next_minute():
    now = datetime.now()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    remaining_seconds = (next_minute - now).total_seconds()
    print(f"Waiting for {remaining_seconds:.2f} seconds until the next full minute...")
    time.sleep(remaining_seconds)

def parse_duration(duration_str):
    # Parses '10 seconds' into 10.0
    units = {'s': 1, 'seconds': 1, 'sec': 1, 'secs': 1, 'second': 1,
             'm': 60, 'minutes': 60, 'min': 60, 'mins': 60, 'minute': 60}
    parts = duration_str.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid duration format: {duration_str}")
    value = float(parts[0])
    unit = parts[1].lower()
    if unit not in units:
        raise ValueError(f"Unknown unit in duration: {unit}")
    return value * units[unit]

def parse_messages_per_window_list(s):
    # Parses '1000,2000,3000' into [1000, 2000, 3000]
    return [int(x.strip()) for x in s.strip().split(',')]

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_producer():
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

def produce_messages():
    producer = create_producer()

    if not producer:
        print("Failed to create Kafka producer. Exiting.")
        return

    window_duration_seconds = parse_duration(WINDOW_DURATION_STR)
    gap_duration_seconds = parse_duration(GAP_DURATION_STR)
    messages_per_window_list = parse_messages_per_window_list(MESSAGES_PER_WINDOW_LIST)

    try:
        window_count = 0

        while True:
            messages_per_window = messages_per_window_list[window_count % len(messages_per_window_list)]
            window_count += 1
            print(f"Starting window {window_count}: Sending {messages_per_window} messages over {window_duration_seconds} seconds")
            window_start_time = time.time()

            if messages_per_window == 0:
                # Wait for the window duration and skip sending messages
                time.sleep(window_duration_seconds)
                continue

            per_message_sleep_time = window_duration_seconds / messages_per_window

            for i in range(messages_per_window):
                data = {
                    'productName': f"product{random.randint(0, 20)}",
                    'id_': f'id{random.randint(0, 100)}',
                    'description': ''.join(random.choices(string.ascii_uppercase + string.digits + ' -', k=random.randint(10, 200))),
                    'priority': random.choice(["low", "normal", "medium", "high"]),
                    'numViews': random.randint(1, 1000),
                    'eventTime': standardize_timestamp_to_milliseconds(time.time()),  # UNIX timestamp in milliseconds
                }
                producer.send(KAFKA_TOPIC, data)
                # Optionally, flush periodically
                # if i % 100 == 0:
                #     producer.flush()
                time.sleep(per_message_sleep_time/2)

            # Ensure we wait until the window duration has elapsed
            elapsed_time = time.time() - window_start_time
            if elapsed_time < window_duration_seconds:
                time.sleep(window_duration_seconds - elapsed_time)

            # For session windows, introduce a gap longer than GAP_DURATION
            if WINDOW_TYPE == 'session':
                # Random gap duration longer than GAP_DURATION
                additional_gap = random.uniform(gap_duration_seconds, gap_duration_seconds * 2)
                print(f"Session window {window_count} completed. Waiting for {additional_gap:.2f} seconds before starting next session.")
                time.sleep(additional_gap)
            else:
                # For tumbling or sliding windows, the producer continues immediately
                continue

    except KeyboardInterrupt:
        print("Stopping producer...")
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    wait_until_next_minute() # wait until the next full minute and then start the process
    produce_messages()