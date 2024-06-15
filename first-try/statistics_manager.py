import faust
from datetime import datetime, timedelta

from Model.DataPoint import DataPoint
from Model.WindowProfiler import initialize_statistics_dictionary


def process_window(window_key, window_profiler):
    """
    A callback function that processes the state of a closed window. Prints data profiling information to the console.
    """

    # the key is a tuple (user_id, (timestamp_start, timestamp_end))
    # the two timestamps correspond to the range of the window that we are processing
    timestamp_start = window_key[1][0]
    timestamp_end = window_key[1][1]
    print("--------------------")
    print(f"{datetime.fromtimestamp(timestamp_start).strftime('%d/%m/%Y, %H:%M:%S')} - {datetime.fromtimestamp(timestamp_end).strftime('%d/%m/%Y, %H:%M:%S')}")
    print(f"Max value        : {window_profiler['max']}")
    print(f"Min value        : {window_profiler['min']}")
    print(f"Sum value        : {window_profiler['sum']}")
    print(f"Sum of squares   : {window_profiler['sum_squares']}")
    print(f"Elements         : {window_profiler['count']}")
    print(f"Distinct         : {len(window_profiler['distinct'])}")


TOPIC = 'input'                     # the name of the source topic to read data from
SINK = 'todo another topic'         # the name of the sink topic to write data to
TABLE = 'statistics'                # the name of the table (window state) that contains statistic information
KAFKA = 'kafka://localhost:9092'    # the address of the message broker, here Kafka is used, hosted locally on port 9092
CLEANUP_INTERVAL = 1.0              # the interval at which faust periodically checks for potential closed windows
WINDOW = timedelta(seconds=10)      # the time size of the window
WINDOW_EXPIRES = timedelta(seconds=1)  # the time after the window closure that the window is considered expired (no more out-of-order accepted)

# declare a faust application with one partition, based on Kafka broker
app = faust.App('statistics-manager', broker=KAFKA, version=1, topic_partitions=1)

# define the cleanup interval, which faust periodically checks for closed windows
app.conf.table_cleanup_interval = CLEANUP_INTERVAL

# define the input topic that our agent reads from
input_topic = app.topic(TOPIC, value_type=DataPoint)
statistics_table = (app.Table(
    name=TABLE,
    default=initialize_statistics_dictionary,
    on_window_close=process_window)
                    .tumbling(WINDOW, expires=WINDOW_EXPIRES))


# define the statistics agent that processes asynchronously every incoming element in the stream
@app.agent(input_topic)
async def statistics_agent(stream):
    async for data_point in stream.group_by(DataPoint.user_id):
        current_dictionary = statistics_table[data_point.user_id].value()

        # if duration is greater than the current max, then update max
        if data_point.duration_watched > statistics_table[data_point.user_id].value()['max']:
            current_dictionary['max'] = data_point.duration_watched

        # if duration is smaller than the current min, then update min
        if data_point.duration_watched < statistics_table[data_point.user_id].value()['min']:
            current_dictionary['min'] = data_point.duration_watched

        # update count, sum, sum of squares and distincts' set
        current_dictionary['count'] = current_dictionary['count'] + 1
        current_dictionary['sum'] = current_dictionary['sum'] + data_point.duration_watched
        current_dictionary['sum_squares'] = current_dictionary['sum_squares'] + data_point.duration_watched**2
        current_dictionary['distinct'].add(str(round(data_point.duration_watched)))

        # Assign back the value to publish in the internal changelog assigning back the value is crucial for restoration
        # in case of a failure. For more information about why this assignment is crucial refer to the official docs:
        # https://faust-streaming.github.io/faust/userguide/tables.html#the-changelog
        statistics_table[data_point.user_id] = current_dictionary


def get_topic() -> faust.TopicT:
    """
    Getter for the input topic object.
    :return: The input topic object that faust agent reads from. Do not modify the returned topic object.
    """
    return input_topic


# to run the worker: <python statistics_manager.py worker -l info> or simply <python statistics_manager.py worker>
if __name__ == '__main__':
    app.main()
