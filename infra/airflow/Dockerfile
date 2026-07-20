FROM apache/airflow:3.2.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir meltano==4.2.0

WORKDIR /opt/airflow/meltano_project

COPY --chown=airflow:root meltano_project/meltano.yml /opt/airflow/meltano_project/meltano.yml
COPY --chown=airflow:root data/input/banvic_raw /opt/airflow/data/input/banvic_raw
COPY --chown=airflow:root dags/banvic_meltano_ingestion_dag.py /opt/airflow/dags/banvic_meltano_ingestion_dag.py

RUN meltano install
