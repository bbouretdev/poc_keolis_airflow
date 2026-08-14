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
            # Limite la taille de chaque instruction INSERT envoyée à Postgres
            "DESTINATION__POSTGRES_DEST__MAX_TEXT_DATA_PAGE_SIZE": "5242880",  # 5 Mo

            # Mode de fonctionnement Storage (Azurite vs Azure Cloud)
            "USE_AZURITE": "{{ params.USE_AZURITE }}",
            "AZURE_STORAGE_CONNECTION_STRING": "{{ conn.get(params.AZURE_CONN_ID).extra_dejson.get('connection_string', '') }}",
            
            # Credentials ADLS Gen2 Native (si mode Cloud)
            "SOURCES__FILESYSTEM__AZURE_STORAGE_ACCOUNT_NAME": "{{ conn.get(params.AZURE_CONN_ID).login }}",
            "SOURCES__FILESYSTEM__AZURE_STORAGE_ACCOUNT_KEY": "{{ conn.get(params.AZURE_CONN_ID).password }}",

            # Destination Postgres (Standardisée)
            "DESTINATION__POSTGRES_DEST__DESTINATION_TYPE": "postgres",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DRIVERNAME": "postgresql",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE": "{{ conn.get(params.POSTGRESQL_CIBLE).schema }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__USERNAME": "{{ conn.get(params.POSTGRESQL_CIBLE).login }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PASSWORD": "{{ conn.get(params.POSTGRESQL_CIBLE).password }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST": "{{ conn.get(params.POSTGRESQL_CIBLE).host }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_CIBLE).port or 5432 }}",

            # Configs spécifiques au flux ADLS
            "DLT_PIPELINE_ID": "{{ params.ID_PIPELINE }}",
            "DLT_AZURE_CONTAINER": "{{ params.CONTENEUR_AZURE }}",
            "DLT_FILE_GLOB": "{{ params.PATTERN_FICHIER }}",
            "DLT_TARGET_SCHEMA": "{{ params.SCHEMA_CIBLE }}",
            "DLT_TARGET_TABLE": "{{ params.TABLE_CIBLE }}",
            "DLT_WRITE_STRATEGY": "{{ params.STRATEGIE_ECRITURE }}",
        }

        git_host = "{{ conn.get(params.GIT_CONN_ID).host }}"

        bash_cmd = f"""
        set -e
        echo "=== Cloning repository ==="
        git clone {git_host} /tmp/repo
        cd /tmp/repo

        echo "=== Running DLT ADLS -> Postgres generic script ==="
        python pipelines/adls_postgres/generic.py
        """

        run_pod = KubernetesPodOperator(
            task_id="run_dlt_adls_ingestion",
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