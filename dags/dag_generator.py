import logging
from pathlib import Path
import yaml

# OBLIGATOIRE : Import explicite pour l'évaluation par le parser Airflow
from airflow import DAG
from airflow.sdk import Param

from factories.postgres_postgres.kubernetes_mode_yaml import create_dag

logger = logging.getLogger(__name__)

# Résolution absolue du dossier courant dans le worktree
CURRENT_DIR = Path(__file__).resolve().parent

# Dossier des configurations YAML
CONFIG_DIR = CURRENT_DIR / "config" / "postgres_postgres"


def build_airflow_params(raw_params: dict) -> dict:
    """Reconstruit le dictionnaire de Param Airflow avec des valeurs par défaut."""
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(
            raw_params.get("ID_PIPELINE", "DEFAULT_ID"),
            type="string",
            title="ID du Pipeline DLT"
        ),
        "GIT_CONN_ID": Param(
            raw_params.get("GIT_CONN_ID", "git-dlt"),
            type="string",
            title="ID de la connexion Airflow Git"
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


# Scan et génération dynamique des DAGs
if CONFIG_DIR.exists():
    for yaml_file in CONFIG_DIR.glob("*.yaml"):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)

            if not pipeline_config or "dag_id" not in pipeline_config:
                continue

            dag_id = pipeline_config["dag_id"]
            formatted_params = build_airflow_params(pipeline_config.get("params", {}))

            generated_dag = create_dag(
                dag_id=dag_id,
                description=pipeline_config.get("description", ""),
                schedule=pipeline_config.get("schedule"),
                params=formatted_params,
            )

            # Enregistrement global pour Airflow
            globals()[dag_id] = generated_dag

        except Exception as e:
            logger.error(f"Erreur lors du traitement du fichier YAML {yaml_file.name}: {e}")
else:
    logger.warning(f"Dossier introuvable : {CONFIG_DIR}")