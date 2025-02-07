# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements/requirements_stream.txt ./requirements.txt

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the kafka producer source code
COPY stream/ .

# Default command to run the producer
CMD ["python", "kafka_stream_generation.py"]