#!/bin/bash

docker compose up -d kafka && sleep 5 && docker exec kafka bash /app/scripts/kafka-entrypoint.sh
