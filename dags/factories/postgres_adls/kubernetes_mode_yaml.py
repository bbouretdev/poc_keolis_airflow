# Dans table_env_vars de la factory, remplace / ajoute la destination Delta :

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
    "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": "{{ conn.get(params.POSTGRESQL_SOURCE).port }}",

    # Variables applicatives DLT
    "DLT_PIPELINE_ID": f"{{{{ params.ID_PIPELINE }}}}_{clean_task_id}",
    "DLT_SOURCE_SCHEMA": "{{ params.SCHEMA_SOURCE }}",
    "DLT_DATASET_NAME": "{{ params.DATASET_NAME }}",
    "DLT_SOURCE_TABLE": table_source,
    "DLT_TARGET_NAME": target_name,
    "DLT_BACKEND": "{{ params.MOTEUR_DLT }}",
    "DLT_CHUNK_SIZE": "{{ params.TAILLE_LOT }}",
    "DLT_WRITE_STRATEGY": "{{ params.STRATEGIE_ECRITURE }}",

    # Destination Delta (URL du conteneur)
    "DESTINATION__DELTA__CREDENTIALS": "az://{{ params.CONTENEUR_AZURE }}",
}