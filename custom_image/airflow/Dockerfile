FROM apache/airflow:3.3.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsasl2-dev \
        heimdal-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow 버전을 함께 고정해 provider 설치 시 Airflow 본체가 바뀌지 않도록 함
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-hdfs \
    apache-airflow-providers-apache-hive
