from datetime import datetime, timedelta, timezone
import sys
import pandas as pd
import random


def create_experiment_input():
    events = []
    timestamp = datetime(year=2024, month=6, day=22, tzinfo=timezone.utc)
    for number in range(1, 2000, 10):
        for i in range(number):
            timestamp += timedelta(seconds=10/number)
            events.append(
                {
                    'timestamp': timestamp,
                    'user_id': str("GLOBAL_USER"),
                    'session_id': str(random.randint(1, 1000000)),
                    'device_id': str(random.randint(1, 1000000)),
                    'video_id': str(random.randint(1, 1000000)),
                    'duration_watched': random.random() * random.randint(1, 100),
                    'genre': random.choice(["Action", "Romance", "Mystery", "Thriller", "Documentary"]),
                    'country': random.choice(["Greece", "Albania", "Costa Rica", "Netherlands"]),
                    'age': random.randint(1, 100),
                    'gender': random.choice(["Male", "Female"]),
                    'subscription_status': random.choice(["Free", "Premium"]),
                    'ratings': random.randint(1, 5),
                    'languages': random.choice(["Greek", "English", "Polish", "Spanish"]),
                    'device_type': random.choice(["Mobile", "Desktop", "Laptop"]),
                    'location': random.choice(["South", "North", "West", "East"]),
                    'playback_quality': random.choice(["4k", "HD", "SD", "480p", "720p", "1080p"]),
                    'interaction_events': random.randint(1, 100)
                }
            )
    return events
