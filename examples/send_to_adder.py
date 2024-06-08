import asyncio
from adder import Add, adding
import random


async def send_value() -> None:
    print(await adding.ask(Add(a=random.randint(0, 10), b=random.randint(0, 10))))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_value())
