import faust
from datetime import datetime, timedelta
from Model.DataPoint import produce_send_actual_data_points
from statistics_manager import get_topic


TOPIC = 'input'
SINK = 'todo another topic'
TABLE = 'statistics'
KAFKA = 'kafka://localhost:9092'
CLEANUP_INTERVAL = 1.0
WINDOW = timedelta(seconds=10)
WINDOW_EXPIRES = timedelta(seconds=1)

app = faust.App('data-generator', broker=KAFKA, version=1, topic_partitions=1)


@app.task()
async def generate_stream_on_the_fly():
    await produce_send_actual_data_points(get_topic())


if __name__ == '__main__':
    app.main()
