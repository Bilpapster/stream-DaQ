import faust
import random
import pandas as pd
from faust import TopicT

VIEWERSHIP_DATASET_KAGGLE = "../datasets/streaming_viewership_data.csv"


class DataPoint(faust.Record):
    """
    A class representing a data point of the Kaggle dataset:
    https://www.kaggle.com/datasets/rajatkumar30/streaming-application-viewership

    Each row in the dataset contains the following information:
    - User_ID: A unique identifier for the user.
    - Session_ID: A unique identifier for the user's session.
    - Device_ID: An identifier for the user's device.
    - Video_ID: An identifier for the video content being viewed.
    - Duration_Watched (minutes): The amount of time (in minutes) the user spent watching the video.
    - Genre: The genre of the video content (e.g., Action, Comedy, Drama, etc.).
    - Country: The country where the interaction event took place.
    - Age: The age of the user.
    - Gender: The gender of the user (e.g., Male, Female).
    - Subscription_Status: The user's subscription status (e.g., Free, Premium).
    - Ratings: The user's rating or feedback for the content (typically on a scale from 1 to 5).
    - Languages: The language of the content being viewed.
    - Device_Type: The type of device used by the user (e.g., Smartphone, Tablet, etc.).
    - Location: The location or city where the interaction event occurred.
    - Playback_Quality: The quality of video playback (e.g., HD, SD, 4K).
    - Interaction_Events: The count of interaction events that occurred during the user's session (e.g., clicks, likes,
    shares, etc.).
    """
    user_id: str
    session_id: str
    device_id: str
    video_id: str
    duration_watched: float
    genre: str
    country: str
    age: int
    gender: str
    subscription_status: str
    ratings: int
    languages: str
    device_type: str
    location: str
    playback_quality: str
    interaction_events: int


def produce_random_data_point() -> DataPoint:
    """
    Use this method to produce random data points. For testing purposes only.
    :return: A random data point.
    """
    return DataPoint(
        user_id=str(random.choice(["UserA", "UserB"])),
        session_id=str(random.randint(1, 1000000)),
        device_id=str(random.randint(1, 1000000)),
        video_id=str(random.randint(1, 1000000)),
        duration_watched=random.random() * random.randint(1, 100),
        genre=random.choice(["Action", "Romance", "Mystery", "Thriller", "Documentary"]),
        country=random.choice(["Greece", "Albania", "Costa Rica", "Netherlands"]),
        age=random.randint(1, 100),
        gender=random.choice(["Male", "Female"]),
        subscription_status=random.choice(["Free", "Premium"]),
        ratings=random.randint(1, 5),
        languages=random.choice(["Greek", "English", "Polish", "Spanish"]),
        device_type=random.choice(["Mobile", "Desktop", "Laptop"]),
        location=random.choice(["South", "North", "West", "East"]),
        playback_quality=random.choice(["4k", "HD", "SD", "480p", "720p", "1080p"]),
        interaction_events=random.randint(1, 100)
    )

def produce_actual_data_point() -> DataPoint:
    """
    Use this method to produce actual data points from the Kaggle dataset.
    :return: The next data point in the dataset. If no point exists, it returns None.
    """
    dataset = pd.read_csv(VIEWERSHIP_DATASET_KAGGLE)
    for row_index in range(len(dataset)):
        row = dataset.iloc[row_index]
        return DataPoint(
            user_id=str(row['User_ID']),
            session_id=str(row['Session_ID']),
            device_id=str(row['Device_ID']),
            video_id=str(row['Video_ID']),
            duration_watched=float(row['Duration_Watched (minutes)']),
            genre=str(row['Genre']),
            country=str(row['Country']),
            age=int(row['Age']),
            gender=str(row['Gender']),
            subscription_status=str(row['Subscription_Status']),
            ratings=int(row['Ratings']),
            languages=str(row['Languages']),
            device_type=str(row['Device_Type']),
            location=str(row['Location']),
            playback_quality=str(row['Playback_Quality']),
            interaction_events=int(row['Interaction_Events'])
        )


async def produce_send_actual_data_points(topic: TopicT) -> None:
    import time

    blocked_events = 0
    dataset = pd.read_csv(VIEWERSHIP_DATASET_KAGGLE)
    for row_index in range(len(dataset)):
        row = dataset.iloc[row_index]

        if random.random() < 0.1:
            blocked_events += 1
            time.sleep(random.randint(1, 3))

        await topic.send(value=DataPoint(
            # user_id=str(row['User_ID']),
            user_id="TestUser",
            session_id=str(row['Session_ID']),
            device_id=str(row['Device_ID']),
            video_id=str(row['Video_ID']),
            duration_watched=float(row['Duration_Watched (minutes)']),
            genre=str(row['Genre']),
            country=str(row['Country']),
            age=int(row['Age']),
            gender=str(row['Gender']),
            subscription_status=str(row['Subscription_Status']),
            ratings=int(row['Ratings']),
            languages=str(row['Languages']),
            device_type=str(row['Device_Type']),
            location=str(row['Location']),
            playback_quality=str(row['Playback_Quality']),
            interaction_events=int(row['Interaction_Events'])
        ))
    print(f"Blocked events: {blocked_events}")


async def produce_send_random_data_for_experiments(topic: TopicT) -> None:
    import time

    start = time.time()
    for index, number in enumerate(range(1, 10**6, 10)):
        desired_start_time = index * 10
        while desired_start_time > time.time() - start:
            time.sleep(0.1)

        points = 0
        for i in range(number):
            # if random.random() < 0.5:
            #     time.sleep(10/number)

            await topic.send(value=DataPoint(
                user_id=str("GLOBAL_USER"),
                session_id=str(random.randint(1, 1000000)),
                device_id=str(random.randint(1, 1000000)),
                video_id=str(random.randint(1, 1000000)),
                duration_watched=random.random() * random.randint(1, 100),
                genre=random.choice(["Action", "Romance", "Mystery", "Thriller", "Documentary"]),
                country=random.choice(["Greece", "Albania", "Costa Rica", "Netherlands"]),
                age=random.randint(1, 100),
                gender=random.choice(["Male", "Female"]),
                subscription_status=random.choice(["Free", "Premium"]),
                ratings=random.randint(1, 5),
                languages=random.choice(["Greek", "English", "Polish", "Spanish"]),
                device_type=random.choice(["Mobile", "Desktop", "Laptop"]),
                location=random.choice(["South", "North", "West", "East"]),
                playback_quality=random.choice(["4k", "HD", "SD", "480p", "720p", "1080p"]),
                interaction_events=random.randint(1, 100)
            ))
            points += 1
        print(f"Points: {points}")
