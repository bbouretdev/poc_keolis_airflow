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

        # 1. Extraction et découpage des tables
        # params["TABLE_SOURCE"] peut être un Param Airflow (rendu au runtime) ou une chaîne/valeur brute.
        # On extrait la valeur brute passée dans params pour la factory.
        raw_table_param = params.get("TABLE_SOURCE")
        table_source_val = raw_table_param.value if hasattr(raw_table_param, "value") else str(raw_table_param or "")

        # Si "*", "orders" ou "orders,items" -> découpage en liste propre
        if not table_source_val or table_source_val.strip() == "*":
            tables_list = ["*"]
        else:
            tables_list = [t.strip() for t in table_source_val.split(",") if t.strip()]

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

        # 2. Boucle pour créer UN POD PAR TABLE
        for table_name in tables_list:
            # Nettoyage du nom de la table pour l'ID de tâche et le nom du Pod K3s
            clean_table_id = table_name.lower().replace("_", "-").replace("*", "all-tables")

            # Copie de l'environnement générique
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

                # Paramètres spécifiques à CETTE TABLE
                "DLT_PIPELINE_ID": f"{{{{ params.ID_PIPELINE }}}}_{clean_table_id}",
                "DLT_SOURCE_SCHEMA": "{{ params.SCHEMA_SOURCE }}",
                "DLT_SOURCE_TABLE": table_name,  # On passe "orders", puis "items" séparément !
                "DLT_TARGET_PATH": "{{ params.CONTENEUR_AZURE }}",
                "DLT_BACKEND": "{{ params.MOTEUR_DLT }}",
                "DLT_CHUNK_SIZE": "{{ params.TAILLE_LOT }}",
            }

            # Instanciation de l'opérateur pour la table en cours
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