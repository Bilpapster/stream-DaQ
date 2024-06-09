import faust
from mode import Service

app = faust.App('service-example')


@app.service
class MyService(Service):

    async def on_start(self):
        print('MY SERVICE IS STARTING')

    async def on_stop(self):
        print('MY SERVICE IS STOPPING')

    @Service.task
    async def _background_task(self):
        while not self.should_stop:
            print('BACKGROUND TASK WAKE UP')
            await self.sleep(1.0)


if __name__ == '__main__':
    app.main()
