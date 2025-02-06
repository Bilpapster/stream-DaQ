# Stage 1: Build the application using Maven
FROM maven:3.8.6-openjdk-11 AS builder

# Set the working directory
WORKDIR /app

# Copy the pom.xml and download dependencies
COPY deequExperiments/pom.xml .
RUN mvn dependency:resolve

# Copy the source code into the container
COPY deequExperiments/src ./src

# Build the application
RUN mvn clean package -DskipTests

# Stage 2: Create the final image that will run the application
FROM openjdk:11-jre-slim

# Install required packages and Spark
ENV SCALA_VERSION=2.12
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin

# Install necessary packages and download Spark
RUN apt-get update && \
    apt-get install -y curl procps && \
    curl -o spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mkdir -p ${SPARK_HOME} && \
    tar -xzf spark.tgz -C /opt && \
    rm spark.tgz && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/* ${SPARK_HOME}/ && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the built application from the builder stage
COPY --from=builder /app/target/skataDeequ-1.0-SNAPSHOT.jar /app/app.jar

# Expose any necessary ports (e.g., for Spark UI)
EXPOSE 4040

#ENTRYPOINT ["spark-submit", "--class", "experiment.StreamingDeequWindows", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_$2.12:$3.5.1", "/app/app.jar"]
ENTRYPOINT spark-submit --class experiment.StreamingDeequWindows --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION} /app/app.jar