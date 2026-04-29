FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-http==4.8.0 \
    apache-airflow-providers-redis==3.6.0 \
    apache-airflow-providers-celery==3.6.0 \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    fastavro==1.9.4 \
    boto3==1.34.0 \
    great-expectations==0.18.8 \
    pyarrow==14.0.2 \
    psycopg2-binary==2.9.9 \
    clickhouse-connect==0.7.0 \
    python-dotenv==1.0.0
