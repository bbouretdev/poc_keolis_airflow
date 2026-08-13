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
            "RUNTIME__WORKERS": "4",

            # Source Postgres (Évaluation Jinja au runtime)
            "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
            "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": "{{ conn.get(params.POSTGRESQL_SOURCE).schema }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": "{{ conn.get(params.POSTGRESQL_SOURCE).login }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": "{{ conn.get(params.POSTGRESQL_SOURCE).password }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": "{{ conn.get(params.POSTGRESQL_SOURCE).host }}",
            "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_SOURCE).port or 5432 }}",

            # Destination Postgres
            "DESTINATION__POSTGRES_DEST__DESTINATION_TYPE": "postgres",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DRIVERNAME": "postgresql",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE": "{{ conn.get(params.POSTGRESQL_CIBLE).schema }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__USERNAME": "{{ conn.get(params.POSTGRESQL_CIBLE).login }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PASSWORD": "{{ conn.get(params.POSTGRESQL_CIBLE).password }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST": "{{ conn.get(params.POSTGRESQL_CIBLE).host }}",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_CIBLE).port or 5432 }}",

            # Params DLT
            "DLT_PIPELINE_ID": "{{ params.ID_PIPELINE }}",
            "DLT_SOURCE_SCHEMA": "{{ params.SCHEMA_SOURCE }}",
            "DLT_SOURCE_TABLE": "{{ params.TABLE_SOURCE }}",
            "DLT_TARGET_SCHEMA": "{{ params.SCHEMA_CIBLE }}",
            "DLT_TARGET_TABLE": "{{ params.TABLE_CIBLE }}",
            "DLT_WRITE_STRATEGY": "{{ params.STRATEGIE_ECRITURE }}",
            "DLT_BACKEND": "{{ params.MOTEUR_DLT }}",
            "DLT_CHUNK_SIZE": "{{ params.TAILLE_LOT }}",
            "DLT_PRIMARY_KEY": "{{ params.CLE_PRIMAIRE | tojson }}",
        }

        # Résolution dynamique de la connexion Git sans "magic string"
        git_host = "{{ conn.get(params.GIT_CONN_ID).host }}"

        bash_cmd = f"""
        set -e
        echo "=== Cloning repository ==="
        git clone {git_host} /tmp/repo
        cd /tmp/repo

        echo "=== Running DLT generic script ==="
        python pipelines/postgres_postgres/generic.py
        """

        run_pod = KubernetesPodOperator(
            task_id="run_dlt_ingestion",
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