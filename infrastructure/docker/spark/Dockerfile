FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.1.0 \
    kafka-python==2.0.2 \
    fastavro==1.9.4 \
    requests==2.31.0 \
    boto3==1.34.0 \
    pyarrow==14.0.2 \
    python-dotenv==1.0.0 \
    psycopg2-binary==2.9.9 \
    clickhouse-connect==0.7.0

ENV SPARK_JARS_DIR=/opt/spark/jars
ENV DELTA_VERSION=3.1.0
ENV KAFKA_VERSION=3.5.1
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.540

RUN curl -fSL -o ${SPARK_JARS_DIR}/delta-spark_2.12-${DELTA_VERSION}.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/delta-storage-${DELTA_VERSION}.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-${KAFKA_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${KAFKA_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/kafka-clients-${KAFKA_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" && \
    curl -fSL -o ${SPARK_JARS_DIR}/spark-avro_2.12-${KAFKA_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${KAFKA_VERSION}/spark-avro_2.12-${KAFKA_VERSION}.jar"

COPY infrastructure/docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER 185
