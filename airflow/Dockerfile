# ════════════════════════════════════════════════════════════════════
# EDF ETL Platform — Image Airflow custom
# ════════════════════════════════════════════════════════════════════
# Base : Airflow 2.9.2 + Python 3.11
# Orchestration Spark via API REST du master (port 6066)
# ➜ Aucun besoin de Java, spark-submit ou docker.io
# ════════════════════════════════════════════════════════════════════

FROM apache/airflow:2.9.2-python3.11

# ── 1. Système : paquets utiles (pas de Java, pas de Docker)
USER root
RUN apt-get update --fix-missing && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── 2. Packages Python (avec contraintes officielles Airflow 2.9.2)
USER airflow
ARG AIRFLOW_VERSION=2.9.2
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
        --default-timeout=300 \
        --retries 10 \
        -r /requirements.txt \
        --constraint "${CONSTRAINT_URL}"

# ── 3. Layout projet
ENV PYTHONPATH="/opt/airflow/project"
