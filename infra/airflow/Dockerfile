FROM apache/airflow:3.2.2@sha256:bef4d135ec7b64658cf64089973edd62cab8fd72203f280c7e0952213c17d912

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir meltano==4.2.0

WORKDIR /opt/airflow/meltano_project

# O meltano.yml fixa os artefatos instalados:
# - tap-csv pelo commit Git validado;
# - target-postgres pela versão publicada validada.
#
# Os lockfiles preservam as definições dos plugins obtidas do Meltano Hub.
COPY --chown=airflow:root \
    meltano_project/meltano.yml \
    /opt/airflow/meltano_project/meltano.yml

COPY --chown=airflow:root \
    meltano_project/plugins \
    /opt/airflow/meltano_project/plugins

COPY --chown=airflow:root \
    data/input/banvic_raw \
    /opt/airflow/data/input/banvic_raw

COPY --chown=airflow:root \
    dags/banvic_meltano_ingestion_dag.py \
    /opt/airflow/dags/banvic_meltano_ingestion_dag.py

RUN meltano install \
    && /opt/airflow/meltano_project/.meltano/extractors/tap-csv/venv/bin/python -c \
       'from importlib.metadata import version; assert version("tap-csv") == "1.2.0"' \
    && /opt/airflow/meltano_project/.meltano/loaders/target-postgres/venv/bin/python -c \
       'from importlib.metadata import version; assert version("meltanolabs-target-postgres") == "0.8.0"' \
    && /opt/airflow/meltano_project/.meltano/extractors/tap-csv/venv/bin/python -c \
       'import json, pathlib; files=list(pathlib.Path("/opt/airflow/meltano_project/.meltano/extractors/tap-csv/venv").glob("lib/python*/site-packages/tap_csv-*.dist-info/direct_url.json")); assert len(files)==1; data=json.loads(files[0].read_text()); assert data["vcs_info"]["commit_id"]=="7af22d8e81ff2ac6bd391aec63fd1fef4eb24b22"'
