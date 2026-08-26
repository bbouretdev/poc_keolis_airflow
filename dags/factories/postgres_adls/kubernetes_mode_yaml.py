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

        raw_tables_param = params.get("TABLES")
        tables_list = raw_tables_param.value if hasattr(raw_tables_param, "value") else raw_tables_param

        if not tables_list or not isinstance(tables_list, list):
            raise ValueError("❌ Le paramètre 'TABLES' doit être une liste non vide dans le YAML.")

        # Extraction propre du paramètre USE_AZURITE (Airflow v3 Param)
        raw_azurite = params.get("USE_AZURITE")
        azurite_val = raw_azurite.value if hasattr(raw_azurite, "value") else raw_azurite
        use_azurite_bool = str(azurite_val).strip().lower() in ("true", "1", "yes")

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

        for table_item in tables_list:
            if not isinstance(table_item, dict) or "source" not in table_item or "target_name" not in table_item:
                raise KeyError("❌ Chaque élément de 'TABLES' doit obligatoirement contenir 'source' et 'target_name'.")

            table_source = table_item["source"]
            target_name = table_item["target_name"]
            
            raw_partition = table_item.get("partition_col")
            partition_col = str(raw_partition) if raw_partition is not None else ""

            clean_task_id = target_name.lower().replace("/", "_").replace("-", "_")

            table_env_vars = {
                "RUNTIME__LOG_LEVEL": "INFO",
                "RUNTIME__DLTHUB_TELEMETRY": "false",
                "RUNTIME__WORKERS": "4",
                "USE_AZURITE": "true" if use_azurite_bool else "false",

                # Source Postgres
                "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
                "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": "{{ conn.get(params.POSTGRESQL_SOURCE).schema }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": "{{ conn.get(params.POSTGRESQL_SOURCE).login }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": "{{ conn.get(params.POSTGRESQL_SOURCE).password }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": "{{ conn.get(params.POSTGRESQL_SOURCE).host }}",
                "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_SOURCE).port }}",

                # Variables applicatives DLT
                "DLT_PIPELINE_ID": f"pg2adls_{clean_task_id}",
                "DLT_SOURCE_SCHEMA": "{{ params.SCHEMA_SOURCE }}",
                "DLT_DATASET_NAME": "{{ params.DATASET_NAME }}",
                "DLT_SOURCE_TABLE": table_source,
                "DLT_TARGET_NAME": target_name,
                "DLT_PARTITION_COL": partition_col,
                "DLT_BACKEND": "{{ params.MOTEUR_DLT }}",
                "DLT_CHUNK_SIZE": "{{ params.TAILLE_LOT }}",
                "DLT_WRITE_STRATEGY": "{{ params.STRATEGIE_ECRITURE }}",

                # Destination DLT Bucket URL simple et propre
                "DESTINATION__FILESYSTEM__BUCKET_URL": "az://{{ params.CONTENEUR_AZURE }}",
            }
            
            if use_azurite_bool:
                az_conn = (
                    "DefaultEndpointsProtocol=http;"
                    "AccountName=devstoreaccount1;"
                    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
                )
                table_env_vars.update({
                    # Connection String globale Azure
                    "AZURE_STORAGE_CONNECTION_STRING": az_conn,

                    # Credentials DLT Python
                    "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
                    "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY": (
                        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                    ),
                    "DESTINATION__FILESYSTEM__CREDENTIALS__CONNECTION_STRING": az_conn,

                    # Variables bas niveau lues par object_store/deltalake
                    "AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
                    "AZURE_STORAGE_ACCOUNT_KEY": (
                        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                    ),
                    "AZURE_STORAGE_USE_EMULATOR": "true",
                    "AZURE_STORAGE_ALLOW_HTTP": "true",
                    "AZURE_BLOB_ENDPOINT": "http://azurite:10000/devstoreaccount1",
                })
            else:
                table_env_vars.update({
                    "AZURE_STORAGE_CONNECTION_STRING": (
                        "{{ (conn.get(params.AZURE_CONN_ID, None) or None) "
                        "and (conn.get(params.AZURE_CONN_ID).extra_dejson or {}).get('connection_string', '') }}"
                    ),
                    "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": (
                        "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).login }}"
                    ),
                    "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY": (
                        "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).password }}"
                    ),
                })

            KubernetesPodOperator(
                task_id=f"run_dlt_{clean_task_id.replace('_', '-')}",
                name=f"dlt-pod-{dag_id}-{clean_task_id}".replace("_", "-").lower(),
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