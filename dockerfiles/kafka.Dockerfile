FROM confluentinc/cp-kafka:latest

# Copy the custom log4j.properties
COPY --chmod=0777 kafka/log4j.properties /etc/kafka/log4j.properties

# copy the entrypoint script that creates topics into the container and make it executable
COPY --chmod=0777 kafka/kafka-entrypoint.sh /kafka-entrypoint.sh

# use the custom entrypoint
ENTRYPOINT ["/kafka-entrypoint.sh"]