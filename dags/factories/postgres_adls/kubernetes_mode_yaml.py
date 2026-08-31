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
            raise ValueError("❌ Le paramètre 'TABLES' doit être une liste non vide.")

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
            src = table_item["source"]
            tgt = table_item["target"]
            exe = table_item["execution"]

            # Extraction par domaine : SOURCE
            table_source = src["table"]
            source_schema = src["schema"]
            enable_windowing_bool = str(src.get("enable_windowing", False)).strip().lower() in ("true", "1", "yes")
            raw_cursor = src.get("incremental_cursor")
            incremental_cursor = str(raw_cursor).strip() if raw_cursor is not None else ""

            # Extraction par domaine : TARGET
            target_name = tgt["name"]
            dataset_name = tgt["dataset"]
            azure_container = tgt["container"]
            storage_format = str(tgt["format"]).lower()
            write_strategy = str(tgt["write_strategy"]).lower()
            
            use_partition_bool = str(tgt.get("use_partition", False)).strip().lower() in ("true", "1", "yes")
            raw_partition = tgt.get("partition_col")
            partition_col = str(raw_partition) if raw_partition is not None else ""

            # Extraction par domaine : EXECUTION
            backend_engine = str(exe["backend"]).lower()
            chunk_size_val = str(exe["chunk_size"])

            clean_task_id = target_name.lower().replace("/", "_").replace("-", "_")
            bucket_url = f"az://{azure_container}"

            # Identifiant STABLE de pipeline dlt pour garantir la persistance du Watermark sur le storage
            stable_pipeline_id = f"{dag_id}__{source_schema}_{table_source}"

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

                # Variables DLT spécifiques à la table (source + fenêtrage)
                "DLT_PIPELINE_ID": stable_pipeline_id,
                "DLT_SOURCE_SCHEMA": source_schema,
                "DLT_SOURCE_TABLE": table_source,
                "DLT_ENABLE_WINDOWING": "true" if enable_windowing_bool else "false",
                "DLT_INCREMENTAL_CURSOR": incremental_cursor,

                # Variables DLT spécifiques à la cible
                "DLT_DATASET_NAME": dataset_name,
                "DLT_TARGET_NAME": target_name,
                "DLT_PARTITION_COL": partition_col,
                "DLT_BACKEND": backend_engine,
                "DLT_CHUNK_SIZE": chunk_size_val,
                "DLT_WRITE_STRATEGY": write_strategy,
                "DLT_STORAGE_FORMAT": storage_format,
                "DLT_USE_PARTITION": "true" if use_partition_bool else "false",

                "DESTINATION__FILESYSTEM__BUCKET_URL": bucket_url,
            }
            
            if use_azurite_bool:
                az_conn = (
                    "DefaultEndpointsProtocol=http;"
                    "AccountName=devstoreaccount1;"
                    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
                )
                table_env_vars.update({
                    "AZURE_STORAGE_CONNECTION_STRING": az_conn,
                    "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
                    "DESTINATION__FILESYSTEM__CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY": (
                        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                    ),
                    "DESTINATION__FILESYSTEM__CREDENTIALS__CONNECTION_STRING": az_conn,

                    "AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
                    "AZURE_STORAGE_ACCOUNT_KEY": (
                        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                    ),
                    "AZURE_STORAGE_ALLOW_HTTP": "true",
                    "AZURE_STORAGE_USE_HTTP": "true",
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