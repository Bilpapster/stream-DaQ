import faust
from Model.DataPoint import produce_send_random_data_for_experiments
from statistics_manager import get_input_topic


KAFKA = 'kafka://localhost:9092'
app = faust.App('data-generator', broker=KAFKA, version=1, topic_partitions=1)


@app.task()
async def generate_stream_on_the_fly():
    await produce_send_random_data_for_experiments(get_input_topic())


if __name__ == '__main__':
    app.main()
