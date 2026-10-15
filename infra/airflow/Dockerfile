FROM apache/airflow:3.1.4

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    odbc-postgresql \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /

RUN pip install --no-cache-dir \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.4/constraints-3.12.txt"

COPY airflowsetting.json /opt/airflow/airflowsetting.json
