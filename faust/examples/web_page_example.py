import faust

app = faust.App('view-example')


@app.page('path/to/view/')
async def myview(web, request):
    print(f'FOO PARAM: {request}')


if __name__ == '__main__':
    app.main()
