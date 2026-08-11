import json
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# Utilisation des nouveaux imports recommandés par Airflow 3.x
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

        # --- 1. Récupération des connexions ---
        # Note : Si vous passez les noms de connexions via des defaults dans params, 
        # on peut extraire leurs valeurs :
        src_conn_id = params["POSTGRESQL_SOURCE"].value if hasattr(params["POSTGRESQL_SOURCE"], "value") else params["POSTGRESQL_SOURCE"]
        dst_conn_id = params["POSTGRESQL_CIBLE"].value if hasattr(params["POSTGRESQL_CIBLE"], "value") else params["POSTGRESQL_CIBLE"]

        src_conn = BaseHook.get_connection(src_conn_id)
        dst_conn = BaseHook.get_connection(dst_conn_id)

        # --- 2. Construction du dictionnaire env_vars ---
        # On lit directement les valeurs par défaut des paramètres
        dlt_env_vars = {
            "RUNTIME__LOG_LEVEL": "INFO",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            "RUNTIME__WORKERS": "4",

            # Source Database
            "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
            "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": str(src_conn.schema or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": str(src_conn.login or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": str(src_conn.password or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": str(src_conn.host or ""),
            "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": str(src_conn.port or 5432),

            # Destination Database
            "DESTINATION__POSTGRES_DEST__DESTINATION_TYPE": "postgres",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DRIVERNAME": "postgresql",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE": str(dst_conn.schema or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__USERNAME": str(dst_conn.login or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PASSWORD": str(dst_conn.password or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST": str(dst_conn.host or ""),
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT": str(dst_conn.port or 5432),

            # Paramètres du DAG (Passage Jinja pour résoudre dynamiquement au Runtime si changé via UI)
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

        git_host = BaseHook.get_connection("git-dlt").host

        bash_cmd = f"""
        set -e
        echo "=== Cloning repository ==="
        git clone {git_host} /tmp/repo
        cd /tmp/repo

        echo "=== Running DLT generic script ==="
        python generic.py
        """

        # --- 3. Instanciation de l'opérateur avec un dict pur ---
        run_pod = KubernetesPodOperator(
            task_id="run_dlt_ingestion",
            name=f"dlt-pod-{dag_id}".replace("_", "-").lower(),
            namespace="default",
            image="dlt-ingestion-engine:dev",
            image_pull_policy="Never",
            env_vars=dlt_env_vars,  # <--- Dictionnaire Python natif (les valeurs individuelles utilisent Jinja)
            cmds=["/bin/bash", "-c"],
            arguments=[bash_cmd],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    return dag