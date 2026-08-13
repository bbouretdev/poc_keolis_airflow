import logging
from pathlib import Path
import yaml

# OBLIGATOIRE : Import explicite pour que le parser Airflow évalue le fichier
from airflow import DAG
from airflow.sdk import Param

from factories.postgres_postgres.kubernetes_mode_yaml import create_dag

logger = logging.getLogger(__name__)

# Résolution exacte du dossier physique du worktree courant
# Exemple dans le Pod : /opt/airflow/dags/.worktrees/<HASH>/dags/
CURRENT_DIR = Path(__file__).resolve().parent

# Dossier des configurations YAML relatif à ce fichier Python
CONFIGS_DIR = CURRENT_DIR / "configs" / "postgres_postgres"


def build_airflow_params(raw_params: dict) -> dict:
    """Reconstruit le dictionnaire de Param d'Airflow depuis les données YAML."""
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
if CONFIGS_DIR.exists():
    yaml_files = list(CONFIGS_DIR.glob("*.yaml"))

    for yaml_file in yaml_files:
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)

            if not pipeline_config or "dag_id" not in pipeline_config:
                continue

            dag_id = pipeline_config["dag_id"]
            formatted_params = build_airflow_params(pipeline_config.get("params", {}))

            # Instanciation via la Factory DLT
            generated_dag = create_dag(
                dag_id=dag_id,
                description=pipeline_config.get("description", ""),
                schedule=pipeline_config.get("schedule"),
                params=formatted_params,
            )

            # Enregistrement dans les variables globales d'Airflow
            globals()[dag_id] = generated_dag

        except Exception as e:
            logger.error(f"Erreur lors du traitement du fichier YAML {yaml_file.name}: {e}", exc_info=True)
else:
    logger.warning(f"Dossier de configuration introuvable dans le worktree : {CONFIGS_DIR}")