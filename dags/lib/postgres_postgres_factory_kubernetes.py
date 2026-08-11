from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
import json

def create_dlt_postgres_dag(dag_id: str, description: str, params: dict, schedule=None):

    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2024, 1, 1),
        schedule=schedule,
        catchup=False,
        params=params,
    ) as dag:

        def build_env_and_run(context):
            # Récupération des connexions Airflow (Dev)
            p = context["params"]
            src_conn = BaseHook.get_connection(p["POSTGRESQL_SOURCE"])
            dst_conn = BaseHook.get_connection(p["POSTGRESQL_CIBLE"])

            env_vars = {
                "RUNTIME__LOG_LEVEL": "INFO",
                "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
                "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": src_conn.schema,
                "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": src_conn.login,
                "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": src_conn.password,
                "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": src_conn.host,
                "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": str(src_conn.port),

                "DESTINATION__POSTGRES_DEST__DESTINATION_TYPE": "postgres",
                "DESTINATION__POSTGRES_DEST__CREDENTIALS__DRIVERNAME": "postgresql",
                "DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE": dst_conn.schema,
                "DESTINATION__POSTGRES_DEST__CREDENTIALS__USERNAME": dst_conn.login,
                "DESTINATION__POSTGRES_DEST__CREDENTIALS__PASSWORD": dst_conn.password,
                "DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST": dst_conn.host,
                "DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT": str(dst_conn.port),

                "DLT_PIPELINE_ID": p["ID_PIPELINE"],
                "DLT_SOURCE_SCHEMA": p["SCHEMA_SOURCE"],
                "DLT_SOURCE_TABLE": p["TABLE_SOURCE"],
                "DLT_TARGET_SCHEMA": p["SCHEMA_CIBLE"],
                "DLT_TARGET_TABLE": p["TABLE_CIBLE"],
                "DLT_WRITE_STRATEGY": p["STRATEGIE_ECRITURE"],
                "DLT_BACKEND": p["MOTEUR_DLT"],
                "DLT_CHUNK_SIZE": str(p["TAILLE_LOT"]),
            }
            if p.get("CLE_PRIMAIRE"):
                env_vars["DLT_PRIMARY_KEY"] = json.dumps(p["CLE_PRIMAIRE"])

            return env_vars

        # Tâche unique exécutée dans le Pod Kubernetes
        git_host = BaseHook.get_connection("git-dlt").host
        
        # Script bash d'exécution dans le Pod
        bash_cmd = f"""
        set -e
        git clone --branch main {git_host} /tmp/repo
        cd /tmp/repo
        python pipelines/postgres_to_postgres.py
        """

        run_pod = KubernetesPodOperator(
            task_id="run_dlt_ingestion",
            name=f"dlt-pod-{dag_id}".replace("_", "-").lower(),
            namespace="default",
            image="dlt-ingestion-engine:dev",
            image_pull_policy="Never",
            env_vars="{{ task_instance.xcom_pull(task_ids='prepare_env') }}", # Injection dynamique des envs
            cmds=["/bin/bash", "-c"],
            arguments=[bash_cmd],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    return dag