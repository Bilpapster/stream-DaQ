import faust
from datetime import timedelta
import sys
from Model.DataPoint import DataPoint

# TODO Check on_window_close function from official example documentation
# TODO https://github.com/faust-streaming/faust/blob/master/examples/windowed_aggregation.py


def todo(key, events):
    timestamp = key[1][0]
    values = [event.value for event in events]
    count = len(values)
    mean = sum(values) / count

    print(
        f'processing window:'
        f'{len(values)} events,'
        f'mean: {mean:.2f},'
        f'timestamp {timestamp}',
    )
    print("This method is not implemented yet. TODO")


# Constant for easy testing and debugging
TIME_DELTA = timedelta(minutes=2)

app = faust.App(id='statistics_manager', broker='kafka://localhost:9092')
input_topic = app.topic('input', value_type=DataPoint)
max_table = app.Table('max_values_per_id', default=(lambda: sys.float_info.min), on_window_close=todo).tumbling(size=TIME_DELTA)
min_table = app.Table('min_values_per_id', default=(lambda: sys.float_info.max), on_window_close=todo).tumbling(size=TIME_DELTA)
sum_table = app.Table('sum_of_values_per_id', default=int, on_window_close=todo).tumbling(size=TIME_DELTA)
count_table = app.Table('count_of_values_per_id', default=int, on_window_close=todo).tumbling(size=TIME_DELTA)
mean_table = app.Table('mean_of_values_per_id', default=int, on_window_close=todo).tumbling(size=TIME_DELTA)


@app.agent(input_topic)
async def max_value_agent(stream):
    async for data_point in stream.group_by(DataPoint.user_id):
        if data_point.duration_watched > max_table[data_point.user_id].value():
            max_table[data_point.user_id] = data_point.duration_watched
            # print(f'Max duration updated for key {data_point.user_id} to {max_table[data_point.user_id].value()}')


@app.agent(input_topic)
async def min_value_agent(stream):
    async for data_point in stream.group_by(DataPoint.user_id):
        if data_point.duration_watched < min_table[data_point.user_id].value():
            min_table[data_point.user_id] = data_point.duration_watched
            # print(f'Min duration updated for key {data_point.user_id} to {min_table[data_point.user_id].value()}')


@app.agent(input_topic)
async def mean_std_value_agent(stream):
    async for data_point in stream.group_by(DataPoint.user_id):
        count_table[data_point.user_id] += 1
        sum_table[data_point.user_id] += data_point.duration_watched
        mean_table[data_point.user_id] = sum_table[data_point.user_id].current() / count_table[
            data_point.user_id].current()


if __name__ == '__main__':
    app.main()
