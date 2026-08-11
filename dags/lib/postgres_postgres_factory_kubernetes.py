import json
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Param
from airflow.sdk.bases.hook import BaseHook


def create_dlt_postgres_dag(
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

        # Récupération sécurisée des ID de connexion depuis les params
        # (Évite les plantages lors de l'analyse du fichier par le Scheduler)
        src_conn_id = params.get("POSTGRESQL_SOURCE")
        if isinstance(src_conn_id, Param):
            src_conn_id = src_conn_id.value

        dst_conn_id = params.get("POSTGRESQL_CIBLE")
        if isinstance(dst_conn_id, Param):
            dst_conn_id = dst_conn_id.value

        # Récupération des informations de connexions Airflow
        src_conn = BaseHook.get_connection(src_conn_id)
        dst_conn = BaseHook.get_connection(dst_conn_id)
        git_conn = BaseHook.get_connection("git-dlt")

        # Construction du dictionnaire de variables d'environnement
        # Les valeurs dynamiques utilisent les expressions Jinja d'Airflow
        dlt_env_vars = {
            "RUNTIME__LOG_LEVEL": "INFO",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            "RUNTIME__WORKERS": "4",

            # Connection Source Postgres
            "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
            "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": str(src_conn.schema or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": str(src_conn.login or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": str(src_conn.password or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": str(src_conn.host or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": str(src_conn.port or 5432),

            # Connection Destination Postgres
            "DESTINATION__POSTGRES_DEST__DESTINATION_TYPE": "postgres",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DRIVERNAME": "postgresql",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE": str(dst_conn.schema or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__USERNAME": str(dst_conn.login or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PASSWORD": str(dst_conn.password or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST": str(dst_conn.host or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT": str(dst_conn.port or 5432),

            # Paramètres évalués au Runtime via Jinja
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

        git_host = git_conn.host

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