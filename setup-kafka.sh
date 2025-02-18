#!/bin/bash

docker compose up -d kafka && docker exec kafka bash /app/scripts/kafka-entrypoint.sh
