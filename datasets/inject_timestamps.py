import pandas as pd
from datetime import datetime, timedelta
import random

from dateutil.tz import tzlocal




def lower_headers_and_inject_timestamps():
    """
    A simple python script that reads viewership dataset and does two operations:
    1. transforms headers to lowercase
    2. inserts an artificial timestamp to every record as integer values (! important to be int for pathway)

    The transformed dataframe is written back in a new .csv file, specified in the OUTPUT_FILE 'constant'.
    """
    INPUT_FILE = 'streaming_viewership_data.csv'
    OUTPUT_FILE = 'streaming_viewership_data_with_timestamps.csv'
    STARTING_DATE_TIME = datetime.now(tz=tzlocal())
    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    RANDOM_ADDITION_SEC_FROM = 1
    RANDOM_ADDITION_SEC_TO = 4


    data = pd.read_csv(INPUT_FILE)
    headers_lowered = [str(header).lower() for header in data.columns]
    data.columns = headers_lowered

    timestamps = []
    for _ in range(data.shape[0]):
        timestamps.append(int(STARTING_DATE_TIME.timestamp()))
        STARTING_DATE_TIME += timedelta(seconds=random.randint(RANDOM_ADDITION_SEC_FROM, RANDOM_ADDITION_SEC_TO))

    data.insert(0, "timestamp", timestamps, True)
    data.to_csv(OUTPUT_FILE, index=False)


if __name__=='__main__':
    lower_headers_and_inject_timestamps()