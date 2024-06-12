import faust


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
