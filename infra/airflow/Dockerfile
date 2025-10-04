FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gettext-base \
 && rm -rf /var/lib/apt/lists/*
USER airflow

# pacotes extras (se tiver)
COPY requirements.txt /requirements.txt
RUN [ -s /requirements.txt ] && pip install --no-cache-dir -r /requirements.txt || true

# leve o settings pra dentro da imagem
COPY airflowsetting.json /opt/airflow/airflowsetting.json