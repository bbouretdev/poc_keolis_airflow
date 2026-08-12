import logging
from pathlib import Path
import yaml

# OBLIGATOIRE : Permet à Airflow d'identifier ce fichier Python comme générateur de DAGs
from airflow import DAG
from airflow.sdk import Param

from factories.postgres_postgres.kubernetes_mode_yaml import create_dag

logger = logging.getLogger(__name__)

# Chemin absolu basé sur l'emplacement réel dans /opt/airflow/dags/
CURRENT_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = CURRENT_DIR / "configs" / "postgres_postgres"


def build_airflow_params(raw_params: dict) -> dict:
    """Reconstruit les objets Param d'Airflow depuis les valeurs YAML."""
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


# Boucle de génération
if CONFIGS_DIR.exists():
    for yaml_file in CONFIGS_DIR.glob("*.yaml"):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)

            if not pipeline_config or "dag_id" not in pipeline_config:
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

            # Enregistrement global obligatoire
            globals()[dag_id] = generated_dag

        except Exception as e:
            logger.error(f"Erreur lors du chargement du fichier YAML {yaml_file.name}: {e}")