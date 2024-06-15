import faust
from datetime import datetime, timedelta

from Model.DataPoint import DataPoint, produce_random_data_point, produce_actual_data_point, produce_send_actual_data_points
from Model.WindowProfiler import initialize_statistics_dictionary


# TODO Check on_window_close function from official example documentation
# TODO https://github.com/faust-streaming/faust/blob/master/examples/windowed_aggregation.py


def process_window(window_key, window_values):
    # print(f"Key of type {type(window_key)}: {window_key}")
    # print(f"Events of type {type(window_values)}: {window_values}")
    timestamp_start = window_key[1][0]
    timestamp_end = window_key[1][1]
    print("--------------------")
    print(f"{datetime.fromtimestamp(timestamp_start).strftime('%d/%m/%Y, %H:%M:%S')} - {datetime.fromtimestamp(timestamp_end).strftime('%d/%m/%Y, %H:%M:%S')}")
    print(f"Max value        : {window_values['max']}")
    print(f"Min value        : {window_values['min']}")
    print(f"Sum value        : {window_values['sum']}")
    print(f"Sum of squares   : {window_values['sum_squares']}")
    print(f"Elements         : {window_values['count']}")
    print(f"Distinct         : {len(window_values['distinct'])}")


TOPIC = 'input'
SINK = 'todo another topic'
TABLE = 'statistics'
KAFKA = 'kafka://localhost:9092'
CLEANUP_INTERVAL = 1.0
WINDOW = timedelta(seconds=10)
WINDOW_EXPIRES = timedelta(seconds=1)

app = faust.App('statistics-manager', broker=KAFKA, version=1, topic_partitions=1)

app.conf.table_cleanup_interval = CLEANUP_INTERVAL
input_topic = app.topic(TOPIC, value_type=DataPoint)
statistics_table = (app.Table(
    name=TABLE,
    default=initialize_statistics_dictionary,
    on_window_close=process_window)
                    .tumbling(WINDOW, expires=WINDOW_EXPIRES))


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

        # update count, sum and sum of squares
        current_dictionary['count'] = current_dictionary['count'] + 1
        current_dictionary['sum'] = current_dictionary['sum'] + data_point.duration_watched
        current_dictionary['sum_squares'] = current_dictionary['sum_squares'] + data_point.duration_watched**2
        current_dictionary['distinct'].add(str(round(data_point.duration_watched)))

        # Assign back the value to publish in the internal changelog assigning back the value is crucial for restoration
        # in case of a failure. For more information about why this assignment is crucial refer to the official docs:
        # https://faust-streaming.github.io/faust/userguide/tables.html#the-changelog
        statistics_table[data_point.user_id] = current_dictionary


# @app.timer(1)
# async def produce():
    # await input_topic.send(value=produce_random_data_point())
    # await input_topic.send(value=produce_actual_data_point())


# @app.task()
# async def generate_stream_on_the_fly():
#     await produce_send_actual_data_points(input_topic)


def get_topic() -> faust.TopicT:
    return input_topic


if __name__ == '__main__':
    app.main()
