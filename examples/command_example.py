import faust

app = faust.App('example-subcommand')


@app.command()
async def example():
    """This docstring is used as the command help in --help"""
    print('Hello from inside the example command')

if __name__ == '__main__':
    app.main()