FROM apache/airflow:2.9.3
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --trusted-host pypi.org --trusted-host files.pythonhosted.org -r /requirements.txt
COPY airflowsetting.json /opt/airflow/airflowsetting.json