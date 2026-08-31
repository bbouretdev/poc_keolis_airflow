import textwrap
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
        api_env_vars = {
            "RUNTIME__LOG_LEVEL": "INFO",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            "USE_AZURITE": "{{ params.USE_AZURITE }}",
            "AZURE_STORAGE_ACCOUNT_NAME": (
                "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).login or '' }}"
            ),
            "AZURE_STORAGE_ACCOUNT_KEY": (
                "{{ (conn.get(params.AZURE_CONN_ID, None) or None) and conn.get(params.AZURE_CONN_ID).password or '' }}"
            ),
            "AZURE_STORAGE_CONNECTION_STRING": (
                "{{ (conn.get(params.AZURE_CONN_ID, None) or None) "
                "and (conn.get(params.AZURE_CONN_ID).extra_dejson or {}).get('connection_string', '') }}"
            ),
            "API_BUCKET_URL": "{{ params.BUCKET_URL }}",
            "API_DATASET_NAME": "{{ params.DATASET_NAME }}",
            "API_PIPELINE_NAME": "{{ params.PIPELINE_NAME }}",
            "API_BASE_URL": "{{ params.BASE_URL }}",
            "API_RESOURCES": "{{ params.RESOURCES | tojson }}",
            "API_LOAD_MODE": "{{ params.LOAD_MODE }}",
            "API_DEFAULT_PARAMS": "{{ params.DEFAULT_PARAMS | tojson }}",
            "API_PRIMARY_KEY": "{{ params.PRIMARY_KEY or '' }}",
            "API_LAYOUT": "{{ params.LAYOUT }}",
            "API_MAX_RETRY_ATTEMPTS": "{{ params.MAX_RETRY_ATTEMPTS }}",
            "API_RETRY_BASE_DELAY": "{{ params.RETRY_BASE_DELAY }}",
        }

        git_host = "{{ conn.get(params.GIT_CONN_ID).host }}"
        git_branch = f"{{{{ conn.get(params.GIT_CONN_ID).extra_dejson.get('branch', 'jbaudrin') }}}}"
        bash_cmd = textwrap.dedent(f"""
        set -e
        echo "=== Cloning repository ==="
        git clone -b {git_branch} {git_host} /tmp/repo
        cd /tmp/repo

        echo "=== Running DLT API -> ADLS export ==="
        python - <<'PY'
        import json
        import os

        from pipelines.api_adls.api_to_adls import run_rest_api_to_adls_pipeline

        run_rest_api_to_adls_pipeline(
            bucket_url=os.environ["API_BUCKET_URL"],
            dataset_name=os.environ["API_DATASET_NAME"],
            pipeline_name=os.environ["API_PIPELINE_NAME"],
            base_url=os.environ["API_BASE_URL"],
            resources=json.loads(os.environ["API_RESOURCES"]),
            load_mode=os.environ["API_LOAD_MODE"],
            default_params=json.loads(os.environ["API_DEFAULT_PARAMS"]),
            primary_key=os.environ["API_PRIMARY_KEY"] or None,
            layout=os.environ["API_LAYOUT"],
            max_retry_attempts=int(os.environ["API_MAX_RETRY_ATTEMPTS"]),
            retry_base_delay=float(os.environ["API_RETRY_BASE_DELAY"]),
        )
        PY
        """)

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