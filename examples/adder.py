import faust
from faust import StreamT
from typing import AsyncIterable

class Add(faust.Record):
    a: int
    b: int


app = faust.App('agent-adder-example')
topic = app.topic('adding', value_type=Add)


@app.agent(topic)
async def adding(stream: StreamT[Add]) -> AsyncIterable[int]:
    async for value in stream:
        yield value.a + value.b
