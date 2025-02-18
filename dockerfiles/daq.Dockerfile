FROM python:3.12-slim

WORKDIR /app

COPY requirements/requirements_daq.txt ./requirements.txt

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY pathway/ .

# Create directory for persisting the results (see also volume in docker-compose.yaml)
CMD mkdir ./data && touch ./data/executionResults.csv

CMD ["/bin/sh", "-c", "\
    echo The value of STREAM environment variable is: $STREAM; \
    if [ \"$STREAM\" = \"reddit\" ]; then \
        echo 'Starting Stream DaQ for Reddit stream processing...'; \
        pathway spawn --processes ${SPARK_NUM_CORES} python ./scalability_experiment_reddit.py; \
    else \
        echo 'Starting Stream DaQ for Products stream processing...'; \
        pathway spawn --processes ${SPARK_NUM_CORES} python ./scalability_experiment.py; \
    fi \
"]