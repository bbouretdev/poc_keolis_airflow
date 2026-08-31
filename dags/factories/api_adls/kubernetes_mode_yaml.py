import json
import textwrap
from datetime import datetime

from airflow import DAG
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


def _resolve_azure_env_vars(params: dict) -> dict:
    use_azurite = str(params.get("USE_AZURITE", "false")).lower() == "true"
    if use_azurite:
        return {
            "USE_AZURITE": "true",
            "AZURE_STORAGE_ACCOUNT_NAME": "devstoreaccount1",
            "AZURE_STORAGE_ACCOUNT_KEY": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            "AZURE_STORAGE_CONNECTION_STRING": (
                "DefaultEndpointsProtocol=http;"
                "AccountName=devstoreaccount1;"
                "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                "BlobEndpoint=http://azurite:10000/devstoreaccount1;"
            ),
        }

    conn_id = params.get("AZURE_CONN_ID", "azure_storage_default")
    conn = Connection.get_connection_from_secrets(conn_id)
    extra = conn.extra_dejson or {}
    return {
        "USE_AZURITE": "false",
        "AZURE_STORAGE_ACCOUNT_NAME": conn.login or "",
        "AZURE_STORAGE_ACCOUNT_KEY": conn.password or "",
        "AZURE_STORAGE_CONNECTION_STRING": extra.get("connection_string", ""),
    }


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
        azure_env = _resolve_azure_env_vars(params)
        git_conn_id = params.get("GIT_CONN_ID", "git-dlt")
        git_conn = Connection.get_connection_from_secrets(git_conn_id)
        git_host = git_conn.host or ""
        git_branch = (git_conn.extra_dejson or {}).get("branch", "jbaudrin")

        api_env_vars = {
            "RUNTIME__LOG_LEVEL": "INFO",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            "USE_AZURITE": azure_env["USE_AZURITE"],
            "AZURE_STORAGE_ACCOUNT_NAME": azure_env["AZURE_STORAGE_ACCOUNT_NAME"],
            "AZURE_STORAGE_ACCOUNT_KEY": azure_env["AZURE_STORAGE_ACCOUNT_KEY"],
            "AZURE_STORAGE_CONNECTION_STRING": azure_env["AZURE_STORAGE_CONNECTION_STRING"],
            "API_BUCKET_URL": str(params.get("BUCKET_URL", "az://target-data")),
            "API_DATASET_NAME": str(params.get("DATASET_NAME", "poke_api")),
            "API_PIPELINE_NAME": str(params.get("PIPELINE_NAME", "poke_api")),
            "API_BASE_URL": str(params.get("BASE_URL", "https://pokeapi.co/api/v2")),
            "API_RESOURCES": json.dumps(params.get("RESOURCES", [])),
            "API_LOAD_MODE": str(params.get("LOAD_MODE", "full")),
            "API_DEFAULT_PARAMS": json.dumps(params.get("DEFAULT_PARAMS", {})),
            "API_PRIMARY_KEY": str(params.get("PRIMARY_KEY") or ""),
            "API_LAYOUT": str(params.get("LAYOUT", "{table_name}")),
            "API_MAX_RETRY_ATTEMPTS": str(params.get("MAX_RETRY_ATTEMPTS", 3)),
            "API_RETRY_BASE_DELAY": str(params.get("RETRY_BASE_DELAY", 5.0)),
        }

        bash_cmd = textwrap.dedent(
            f"""
            set -e
            echo "=== Cloning repository ==="
            git clone -b {git_branch} {git_host} /tmp/repo
            cd /tmp/repo

            echo "=== Running DLT API -> ADLS export ==="
            python pipelines/api_adls/api_to_adls_bis.py
            """
        )

        KubernetesPodOperator(
            task_id="run_dlt_api_export",
            name=f"dlt-pod-{dag_id}".replace("_", "-").lower(),
            namespace="airflow",
            image="dlt-ingestion-engine:dev",
            image_pull_policy="Never",
            env_vars=api_env_vars,
            cmds=["/bin/bash", "-c"],
            arguments=[bash_cmd],
            get_logs=True,
            is_delete_operator_pod=True,
        )

    return dag