from datetime import datetime, timedelta, timezone
import sys
import pandas as pd
import random


def create_input():
    """
    Auxiliary function that creates data stream for bytewax to consume.
    :return: A list of event logs in increasing timestamp order.
    """
    sys.path.append('../datasets')
    dataset = pd.read_csv('../datasets/streaming_viewership_data.csv')

    events = []
    timestamp = datetime(year=2024, month=6, day=22, tzinfo=timezone.utc)
    for row_index in range(len(dataset)):
        # get the next row of the csv dataset
        row = dataset.iloc[row_index]

        # with probability 0.1, add some delay to the occurrence of the event
        if random.random() < 0.1:
            timestamp += timedelta(seconds=random.randint(1, 3))

        # pack the timestamp with the data values and append the event to the list
        events.append(
            {
                "timestamp": timestamp,
                "user_id": "TestUser",
                "session_id": str(row['Session_ID']),
                "device_id": str(row['Device_ID']),
                "video_id": str(row['Video_ID']),
                "duration_watched": float(row['Duration_Watched (minutes)']),
                "genre": str(row['Genre']),
                "country": str(row['Country']),
                "age": int(row['Age']),
                "gender": str(row['Gender']),
                "subscription_status": str(row['Subscription_Status']),
                "ratings": int(row['Ratings']),
                "languages": str(row['Languages']),
                "device_type": str(row['Device_Type']),
                "location": str(row['Location']),
                "playback_quality": str(row['Playback_Quality']),
                "interaction_events": int(row['Interaction_Events'])
            }
        )
    return events
