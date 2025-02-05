FROM confluentinc/cp-kafka:latest

# copy the entrypoint script that creates topics into the container and make it executable
COPY --chmod=0755 kafka/kafka-entrypoint.sh /kafka-entrypoint.sh

# use the custom entrypoint
ENTRYPOINT ["/kafka-entrypoint.sh"]