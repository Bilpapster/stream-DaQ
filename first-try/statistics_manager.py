import faust
from datetime import timedelta
import sys


class DataPoint(faust.Record):
    id: str
    timestamp: float
    value: float


app = faust.App(id='statistics_manager', broker='kafka://localhost:9092')
input_topic = app.topic('input', value_type=DataPoint)
# max_table = app.Table('max_values_per_id', default=(lambda: sys.float_info.min)).tumbling(size=timedelta(seconds=30))
# min_table = app.Table('min_values_per_id', default=(lambda: sys.float_info.max)).tumbling(size=timedelta(seconds=30))


@app.agent(input_topic)
async def max_value_agent(stream):
    async for data_point in stream.group_by(DataPoint.id):
        print(data_point)
        # if data_point.value > max_table[data_point.id]:
        #     max_table[data_point.id] = data_point.value
        #     print(max_table[data_point.id])


# @app.agent(input_topic)
# async def min_value_agent(stream):
#     async for data_point in stream.group_by(DataPoint.id):
#         if data_point.value < min_table[data_point.id]:
#             min_table[data_point.id] = data_point.value
#             print(min_table[data_point.id])


if __name__ == '__main__':
    app.main()
