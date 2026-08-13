import logging
from pathlib import Path
import yaml

# OBLIGATOIRE : Présence de DAG pour qu'Airflow évalue ce fichier
from airflow import DAG
from airflow.sdk import Param

from factories.postgres_postgres.kubernetes_mode_yaml import create_dag

logger = logging.getLogger(__name__)

# Résolution absolue du dossier courant
CURRENT_DIR = Path(__file__).resolve().parent

# CORRECTION : 'config' au singulier !
CONFIG_DIR = CURRENT_DIR / "config" / "postgres_postgres"


def build_airflow_params(raw_params: dict) -> dict:
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(raw_params.get("ID_PIPELINE", "DEFAULT_ID"), type="string", title="ID du Pipeline DLT"),
        "POSTGRESQL_SOURCE": Param(raw_params.get("POSTGRESQL_SOURCE", "postgres_source"), type="string", title="Connexion Source"),
        "POSTGRESQL_CIBLE": Param(raw_params.get("POSTGRESQL_CIBLE", "postgres_target"), type="string", title="Connexion Cible"),
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
        "CLE_PRIMAIRE": Param(raw_params.get("CLE_PRIMAIRE"), type=["array", "null"], title="Clé(s) primaire(s)"),
        "MOTEUR_DLT": Param(
            default=raw_params.get("MOTEUR_DLT", "connectorx"),
            type="string",
            enum=["connectorx", "pyarrow", "pandas"],
            title="Moteur d'extraction DLT"
        ),
        "TAILLE_LOT": Param(raw_params.get("TAILLE_LOT", 100000), type="integer", title="Taille des lots"),
    }


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

            globals()[dag_id] = generated_dag

        except Exception as e:
            logger.error(f"Erreur lors du traitement du fichier {yaml_file.name}: {e}")
else:
    logger.warning(f"Dossier introuvable : {CONFIG_DIR}")