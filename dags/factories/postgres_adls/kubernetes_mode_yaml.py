import json
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def create_dag(
    dag_id: str,
    description: str,
    params: dict,
    schedule=None,
):

    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2024, 1, 1),
        schedule=schedule,
        catchup=False,
        params=params,
    ) as dag:

        dlt_env_vars = {
            "RUNTIME__LOG_LEVEL": "INFO",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            "RUNTIME__WORKERS": "1",

            # 1. Source Postgres
            "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
            "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": "{{ conn.get(params.POSTGRESQL_SOURCE).schema }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": "{{ conn.get(params.POSTGRESQL_SOURCE).login }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": "{{ conn.get(params.POSTGRESQL_SOURCE).password }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": "{{ conn.get(params.POSTGRESQL_SOURCE).host }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_SOURCE).port or 5432 }}",

            # 2. Storage Azure / Azurite Destination
            "USE_AZURITE": "{{ params.USE_AZURITE }}",
            "AZURE_STORAGE_CONNECTION_STRING": (
                "{{ (conn.get(params.AZURE_CONN_ID, None) or None) "
                "and (conn.get(params.AZURE_CONN_ID).extra_dejson or {}).get('connection_string', '') }}"
            ),
            
            # Destination DLT Native Filesystem (si Prod ADLS Gen2)
            "DESTINATION__FILESYSTEM__BUCKET_URL": "az://{{ params.CONTENEUR_AZURE }}",
            "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": (
                "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).login or '' }}"
            ),
            "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY": (
                "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).password or '' }}"
            ),

            # 3. Paramètres applicatifs du script export
            "DLT_PIPELINE_ID": "{{ params.ID_PIPELINE }}",
            "DLT_SOURCE_SCHEMA": "{{ params.SCHEMA_SOURCE }}",
            "DLT_SOURCE_TABLE": "{{ params.TABLE_SOURCE }}",  # Transmet "orders", "orders,items" ou "*"
            "DLT_TARGET_PATH": "{{ params.CONTENEUR_AZURE }}",
            "DLT_BACKEND": "{{ params.MOTEUR_DLT }}",
            "DLT_CHUNK_SIZE": "{{ params.TAILLE_LOT }}",
        }

        git_host = "{{ conn.get(params.GIT_CONN_ID).host }}"

        bash_cmd = f"""
        set -e
        echo "=== Cloning repository ==="
        git clone {git_host} /tmp/repo
        cd /tmp/repo

        echo "=== Running DLT Postgres -> ADLS export script ==="
        python pipelines/postgres_adls/generic.py
        """

        run_pod = KubernetesPodOperator(
            task_id="run_dlt_postgres_export",
            name=f"dlt-pod-{dag_id}".replace("_", "-").lower(),
            namespace="airflow",
            image="dlt-ingestion-engine:dev",
            image_pull_policy="Never",
            env_vars=dlt_env_vars,
            cmds=["/bin/bash", "-c"],
            arguments=[bash_cmd],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    return dag