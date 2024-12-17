FROM openjdk:8
LABEL maintainer="Your Name <youremail@example.com>"

# Install Spark
ENV SPARK_VERSION=3.4.0
RUN apt-get update && \
    apt-get install -y curl && \
    curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz



# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Scala
ENV SCALA_VERSION=2.12.17
RUN curl -O https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.deb && \
    dpkg -i scala-${SCALA_VERSION}.deb && \
    rm scala-${SCALA_VERSION}.deb

# Set working directory
WORKDIR /app

# Copy your Scala files into the container
COPY . /app
