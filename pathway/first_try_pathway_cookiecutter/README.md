# Local development

Start with building your containers: `docker compose -f local.yml build`.

You are ready to start developing your application!
Define your custom logic in `first_try_pathway_cookiecutter/pipeline.py`. It already contains a sample code which sums all the input values.

You can test it in the following modes:

- [debug (batch mode)] run your Pathway app code with pytest with `docker compose -f local.yml run --rm pathway_app pytest`
- [streaming] run your Pathway app `docker compose -f local.yml up`. Modify `InfiniteStream` in `first_try_pathway_cookiecutter/input.py` to feed it with different data. The results are streamed to the `output.csv` file (you can change this in `first_try_pathway_cookiecutter/output.py`)

# Production environment

Production environment streams data from `kafka`.
Build production containers with `docker compose -f prod.yml build`

To run your application invoke:
1. `docker compose -f prod.yml rm -svf` to clean the state so that `kafka` can start without issues
2. `docker compose -f prod.yml up`

For test, you can push messages to kafka by running
`docker compose -f prod.yml exec kafka kafka-console-producer --bootstrap-server kafka:9092 --topic input`

and typing in the messages, e.g:
`{"value":10}`


# Configuration

Supply configuration with environment variables.

For ease of development, you can also use dotenv file in `config/.env` to specify configuration.
Note that environment variables will take precedence over any configuration specified in `config/.env` file.
