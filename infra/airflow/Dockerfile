FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gettext-base \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
ARG AIRFLOW_VERSION=2.9.3

# Descobre "major.minor" do Python (ex.: 3.12) e usa no constraints
RUN PY_MM="$(python -c 'import sys; print(\".\".join(map(str, sys.version_info[:2])))')" && \
    if [ -s /requirements.txt ]; then \
      pip install --no-cache-dir -r /requirements.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints/${AIRFLOW_VERSION}/constraints-${PY_MM}.txt"; \
    fi

COPY airflowsetting.json /opt/airflow/airflowsetting.json
