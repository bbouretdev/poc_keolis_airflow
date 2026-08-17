from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

def parse_mapping(pattern_raw: str) -> list:
    """Découpe 'users_test.parquet:users, items.parquet:items' en liste de tuples [(fichier, table)]."""
    items = []
    for token in pattern_raw.split(","):
        if ":" in token:
            file_pattern, table_name = token.split(":", 1)
            items.append((file_pattern.strip(), table_name.strip()))
        else:
            p = token.strip()
            items.append((p, p.replace(".parquet", "")))
    return items

def create_dag(dag_id: str, description: str, schedule, params: dict) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        params=params,
        catchup=False,
    ) as dag:

        # Extraction des couples (fichier, table)
        pattern_raw = params["PATTERN_FICHIER"].value if hasattr(params["PATTERN_FICHIER"], "value") else params["PATTERN_FICHIER"]
        file_mappings = parse_mapping(pattern_raw)

        previous_task = None

        for file_pattern, table_name in file_mappings:
            base_pipeline_id = params["ID_PIPELINE"].value if hasattr(params["ID_PIPELINE"], "value") else params["ID_PIPELINE"]
            
            # 🔴 ID de pipeline DLT unique et isolé par table
            table_pipeline_id = f"{base_pipeline_id}__{table_name}"

            # Copie des variables d'environnement ajustées pour ce Pod spécifique
            task_env_vars = dlt_env_vars.copy()
            task_env_vars["DLT_FILE_GLOB"] = f"{file_pattern}:{table_name}"
            task_env_vars["DLT_PIPELINE_ID"] = table_pipeline_id
            
            # Bridages de sécurité pour Postgres
            task_env_vars["RUNTIME__WORKERS"] = "1"
            task_env_vars["DESTINATION__POSTGRES_DEST__MAX_TEXT_DATA_PAGE_SIZE"] = "1048576"

            # 🔴 Tâche Airflow = 1 Pod K8s dédié
            task = KubernetesPodOperator(
                task_id=f"ingest_{table_name}",
                name=f"dlt-{dag_id}-{table_name}".replace("_", "-").replace(".", "-").lower(),
                namespace="airflow",
                image="dlt-ingestion-engine:dev",
                image_pull_policy="Never",
                env_vars=task_env_vars,
                cmds=["/bin/bash", "-c"],
                arguments=[bash_cmd],
                get_logs=True,
                is_delete_operator_pod=True,
            )

            # 🔴 Chainage Sécurité : Exécution Séquentielle (Pod 1 puis Pod 2)
            if previous_task:
                previous_task >> task
            previous_task = task

    return dag