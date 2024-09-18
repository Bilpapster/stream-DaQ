import pathway as pw


def generate_artificial_range_stream(number_of_rows: int = 50, offset: int = 10, input_rate: float = 1.0):
    """
    A utility method to generate an artificial stream of increasing numbers.
    :param number_of_rows: The number of rows to generate. If set to None, the stream will be generated infinitely.
    Defaults to 50 values.
    :param offset: The number to start generating from. Defaults to 10.
    :param input_rate: The rate with which the values of the stream will be generated. Defaults to 1 value per second.
    :return: a stream of increasing integer values.
    """
    artificial_range_stream = pw.demo.range_stream(nb_rows=number_of_rows, offset=offset, input_rate=input_rate)
    return artificial_range_stream


def generate_artificial_noisy_linear_stream(number_of_rows:int = 50, input_rate: float = 1.0):
    """
    A utility method to generate an artificial stream of noisy linear points in the x-y plane.
    :param number_of_rows: The number of rows (x-y pairs) to generate. If set to None, the stream will be generated
    infinitely. Defaults to 50 pairs.
    :param input_rate: The rate with which the pairs of the streams will be generated. Defaults to 1 pair per second.
    :return: a stream of linear noisy x-y points.
    """
    artificial_noisy_linear_stream = pw.demo.noisy_linear_stream(nb_rows=number_of_rows, input_rate=input_rate)
    return artificial_noisy_linear_stream

class ViewershipSchema(pw.Schema):
    timestamp: str
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

def generate_artificial_random_viewership_data_stream(number_of_rows:int = 50, input_rate: float = 1.0):
    """
    A utility method to generate an artificial stream of random viewership data, based on the viewership data schema.
    :param number_of_rows: the number of random data points to generate.
    :param input_rate: the rate with which the data points will be generated. Defaults to 1 data point per second.
    :return: An artificial, randomly generated stream of viewership data for testing purposes only.
    """
    import random
    from datetime import datetime, timedelta

    DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    value_functions = {
        'timestamp': lambda _: (datetime.now() - timedelta(days=60) + timedelta(seconds=_)).strftime(DATE_TIME_FORMAT),
        'user_id': lambda _: str(random.choice(["UserA", "UserB"])),
        'session_id': lambda _: str(random.randint(1, 1000000)),
        'device_id': lambda _: str(random.randint(1, 1000000)),
        'video_id': lambda _: str(random.randint(1, 1000000)),
        'duration_watched': lambda _: random.random() * random.randint(1, 1000000),
        'genre': lambda _: random.choice(["Action", "Romance", "Mystery", "Thriller", "Documentary"]),
        'country': lambda _: random.choice(["Greece", "Albania", "Costa Rica", "Netherlands"]),
        'age': lambda _: random.randint(1, 100),
        'gender': lambda _: random.choice(["Male", "Female"]),
        'subscription_status': lambda _: random.choice(["Free", "Premium"]),
        'ratings': lambda _: random.randint(1, 5),
        'languages': lambda _: random.choice(["Greek", "English", "Polish", "Spanish"]),
        'device_type': lambda _: random.choice(["Mobile", "Desktop", "Laptop"]),
        'location': lambda _: random.choice(["South", "North", "West", "East"]),
        'playback_quality': lambda _: random.choice(["4k", "HD", "SD", "480p", "720p", "1080p"]),
        'interaction_events': lambda _: random.randint(1, 10)
    }

    artificial_viewership_stream = pw.demo.generate_custom_stream(value_generators=value_functions, nb_rows=number_of_rows,
                                                                  schema=ViewershipSchema, input_rate=input_rate)
    return artificial_viewership_stream


def generate_artificial_actual_viewership_data_stream(path:str, schema: pw.Schema = ViewershipSchema,
                                                      input_rate: float = 1.0):
    artificial_viewership_stream = pw.demo.replay_csv(path, schema=schema, input_rate=input_rate)
    return artificial_viewership_stream
