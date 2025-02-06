FROM maven:3.8.6-openjdk-11 AS builder

WORKDIR /app

COPY deequExperiments/pom.xml .
RUN mvn dependency:resolve

COPY deequExperiments/src ./src

RUN mvn clean package -DskipTests

FROM openjdk:11-jre-slim

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

WORKDIR /app

COPY --from=builder /app/target/skataDeequ-1.0-SNAPSHOT.jar /app/app.jar

EXPOSE 4040

ENTRYPOINT spark-submit --class experiment.StreamingDeequWindows --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION} /app/app.jar