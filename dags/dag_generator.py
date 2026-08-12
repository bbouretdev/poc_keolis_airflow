import logging
from pathlib import Path
import yaml
from airflow.sdk import Param

# On importe la factory
from factories.postgres_postgres.kubernetes_mode import create_dag

logger = logging.getLogger(__name__)

# Le dossier où l'on range nos YAML
CONFIGS_DIR = Path(__file__).parent / "configs" / "postgres_postgres"


def build_airflow_params(raw_params: dict) -> dict:
    return {
        "ID_PIPELINE": Param(raw_params.get("ID_PIPELINE"), type="string", title="ID du Pipeline DLT"),
        "POSTGRESQL_SOURCE": Param(raw_params.get("POSTGRESQL_SOURCE", "postgres_source"), type="string", title="Connexion Source"),
        "POSTGRESQL_CIBLE": Param(raw_params.get("POSTGRESQL_CIBLE", "postgres_cible"), type="string", title="Connexion Cible"),
        "SCHEMA_SOURCE": Param(raw_params.get("SCHEMA_SOURCE", "dlt"), type="string"),
        "TABLE_SOURCE": Param(raw_params.get("TABLE_SOURCE"), type="string"),
        "SCHEMA_CIBLE": Param(raw_params.get("SCHEMA_CIBLE", "dlt"), type="string"),
        "TABLE_CIBLE": Param(raw_params.get("TABLE_CIBLE"), type="string"),
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


if CONFIGS_DIR.exists():
    for yaml_file in CONFIGS_DIR.glob("*.yaml"):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                pipeline_config = yaml.safe_load(f)

            if not pipeline_config or "dag_id" not in pipeline_config:
                continue

            dag_id = pipeline_config["dag_id"]
            formatted_params = build_airflow_params(pipeline_config.get("params", {}))

            # On instancie le DAG via la factory
            generated_dag = create_dag(
                dag_id=dag_id,
                description=pipeline_config.get("description", ""),
                schedule=pipeline_config.get("schedule"),
                params=formatted_params,
            )

            # On l'enregistre dans les globales pour qu'Airflow le détecte
            globals()[f"dag_{dag_id}"] = generated_dag

        except Exception as e:
            logger.error(f"Erreur lors du chargement du fichier YAML {yaml_file.name}: {e}")