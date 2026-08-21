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
        max_active_tasks=3,
        catchup=False,
        params=params,
    ) as dag:

        # 1. Extraction de la liste des tables depuis le dictionnaire params
        raw_tables_param = params.get("TABLES")
        tables_list = raw_tables_param.value if hasattr(raw_tables_param, "value") else (raw_tables_param or [])

        git_host = "{{ conn.get(params.GIT_CONN_ID).host }}"
        git_branch = f"{{{{ conn.get(params.GIT_CONN_ID).extra_dejson.get('branch', 'main') }}}}"

        bash_cmd = f"""
        set -e
        echo "=== Cloning repository ==="
        git clone -b {git_branch} {git_host} /tmp/repo
        cd /tmp/repo

        echo "=== Running DLT Postgres -> ADLS export script ==="
        python pipelines/postgres_adls/generic.py
        """

        # 2. Boucle : 1 Pod Kubernetes par entrée dans la liste TABLES
        for table_item in tables_list:
            # Sécurité si l'élément est un dictionnaire ou une simple string
            if isinstance(table_item, dict):
                table_source = table_item.get("source")
                target_name = table_item.get("target_name", table_source)
            else:
                table_source = str(table_item)
                target_name = table_source

            clean_table_id = table_source.lower().replace("_", "-")

            table_env_vars = {
                "RUNTIME__LOG_LEVEL": "INFO",
                "RUNTIME__DLTHUB_TELEMETRY": "false",
                "RUNTIME__WORKERS": "4",

                # Source Postgres
                "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
                "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": "{{ conn.get(params.POSTGRESQL_SOURCE).schema }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": "{{ conn.get(params.POSTGRESQL_SOURCE).login }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": "{{ conn.get(params.POSTGRESQL_SOURCE).password }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": "{{ conn.get(params.POSTGRESQL_SOURCE).host }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_SOURCE).port or 5432 }}",

                # Storage Azure / Azurite Destination
                "USE_AZURITE": "{{ params.USE_AZURITE }}",
                "AZURE_STORAGE_CONNECTION_STRING": (
                    "{{ (conn.get(params.AZURE_CONN_ID, None) or None) "
                    "and (conn.get(params.AZURE_CONN_ID).extra_dejson or {}).get('connection_string', '') }}"
                ),
                "DESTINATION__FILESYSTEM__BUCKET_URL": "az://{{ params.CONTENEUR_AZURE }}",
                "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": (
                    "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).login or '' }}"
                ),
                "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY": (
                    "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).password or '' }}"
                ),

                # Paramètres applicatifs transmis à ton script Python DLT
                "DLT_PIPELINE_ID": f"{{{{ params.ID_PIPELINE }}}}_{clean_table_id}",
                "DLT_SOURCE_SCHEMA": "{{ params.SCHEMA_SOURCE }}",
                "DLT_SOURCE_TABLE": table_source,       # ex: "orders"
                "DLT_TARGET_NAME": target_name,         # ex: "commandes_export"
                "DLT_TARGET_PATH": "{{ params.CONTENEUR_AZURE }}",
                "DLT_BACKEND": "{{ params.MOTEUR_DLT }}",
                "DLT_CHUNK_SIZE": "{{ params.TAILLE_LOT }}",
            }

            KubernetesPodOperator(
                task_id=f"run_dlt_{clean_table_id}",
                name=f"dlt-pod-{dag_id}-{clean_table_id}".replace("_", "-").lower(),
                namespace="airflow",
                image="dlt-ingestion-engine:dev",
                image_pull_policy="Never",
                env_vars=table_env_vars,
                cmds=["/bin/bash", "-c"],
                arguments=[bash_cmd],
                get_logs=True,
                is_delete_operator_pod=True,
                dag=dag,
            )

    return dag