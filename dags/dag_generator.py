import logging
from pathlib import Path
import yaml

# OBLIGATOIRE : Présence explicite de DAG pour que le parser Airflow évalue le fichier
from airflow import DAG
from airflow.sdk import Param

from factories.postgres_postgres.kubernetes_mode_yaml import create_dag

logger = logging.getLogger(__name__)

# Racine du bundle de DAGs (emplacement de dag_generator.py)
BASE_DIR = Path(__file__).resolve().parent

# Recherche récursive de TOUS les fichiers .yaml situés dans n'importe quel dossier 'configs'
yaml_files = list(BASE_DIR.rglob("configs/**/*.yaml"))

print(f"=== [DAG_GENERATOR] Scan de la racine : {BASE_DIR} ===")
print(f"=== [DAG_GENERATOR] {len(yaml_files)} fichier(s) YAML trouvé(s) : {[f.name for f in yaml_files]} ===")


def build_airflow_params(raw_params: dict) -> dict:
    """Reconstruit le dictionnaire de Param Airflow à partir de la configuration YAML."""
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(
            raw_params.get("ID_PIPELINE", "DEFAULT_ID"),
            type="string",
            title="ID du Pipeline DLT"
        ),
        "POSTGRESQL_SOURCE": Param(
            raw_params.get("POSTGRESQL_SOURCE", "postgres_source"),
            type="string",
            title="Connexion Source"
        ),
        "POSTGRESQL_CIBLE": Param(
            raw_params.get("POSTGRESQL_CIBLE", "postgres_target"),
            type="string",
            title="Connexion Cible"
        ),
        "SCHEMA_SOURCE": Param(raw_params.get("SCHEMA_SOURCE", "dlt"), type="string"),
        "TABLE_SOURCE": Param(raw_params.get("TABLE_SOURCE", "orders"), type="string"),
        "SCHEMA_CIBLE": Param(raw_params.get("SCHEMA_CIBLE", "dlt"), type="string"),
        "TABLE_CIBLE": Param(raw_params.get("TABLE_CIBLE", "orders"), type="string"),
        "STRATEGIE_ECRITURE": Param(
            default=raw_params.get("STRATEGIE_ECRITURE", "REPLACE"),
            type="string",
            enum=["REPLACE", "APPEND", "UPDATE"],
            title="Stratégie d'écriture"
        ),
        "CLE_PRIMAIRE": Param(
            raw_params.get("CLE_PRIMAIRE"),
            type=["array", "null"],
            title="Clé(s) primaire(s)"
        ),
        "MOTEUR_DLT": Param(
            default=raw_params.get("MOTEUR_DLT", "connectorx"),
            type="string",
            enum=["connectorx", "pyarrow", "pandas"],
            title="Moteur d'extraction DLT"
        ),
        "TAILLE_LOT": Param(
            raw_params.get("TAILLE_LOT", 100000),
            type="integer",
            title="Taille des lots"
        ),
    }


# Génération dynamique des DAGs
for yaml_file in yaml_files:
    try:
        with open(yaml_file, "r", encoding="utf-8") as f:
            pipeline_config = yaml.safe_load(f)

        if not pipeline_config or "dag_id" not in pipeline_config:
            print(f"=== [DAG_GENERATOR] Ignoré (pas de dag_id) : {yaml_file.name} ===")
            continue

        dag_id = pipeline_config["dag_id"]
        formatted_params = build_airflow_params(pipeline_config.get("params", {}))

        # Instanciation via la Factory
        generated_dag = create_dag(
            dag_id=dag_id,
            description=pipeline_config.get("description", ""),
            schedule=pipeline_config.get("schedule"),
            params=formatted_params,
        )

        # Enregistrement dans les variables globales pour Airflow
        globals()[dag_id] = generated_dag
        print(f"=== [DAG_GENERATOR] DAG '{dag_id}' créé avec succès depuis {yaml_file.name} ===")

    except Exception as e:
        logger.error(f"Erreur lors du traitement du fichier {yaml_file.name}: {e}", exc_info=True)