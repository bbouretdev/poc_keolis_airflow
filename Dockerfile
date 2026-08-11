FROM apache/airflow:2.8.1-python3.11

USER root
# Installation de git et des outils système si besoin
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Pre-installation de dlt et des extras requis pour vos sources/destinations
RUN pip install --no-cache-dir \
    "dlt[postgres,duckdb,azure,snowflake]" \
    azure-identity \
    azure-keyvault-secrets

RUN pip install --no-cache-dir \
    "dlt[postgres,duckdb,azure,snowflake]" \
    connectorx \
    azure-identity \
    azure-keyvault-secrets