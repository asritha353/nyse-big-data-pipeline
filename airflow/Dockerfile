FROM apache/airflow:3.3.1

COPY requirements.txt /tmp/project-requirements.txt
COPY airflow/requirements.txt /tmp/airflow-requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    -r /tmp/project-requirements.txt \
    -r /tmp/airflow-requirements.txt
