import logging
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Param

from factories.postgres_adls.kubernetes_mode_yaml import create_dag as create_pg2adls_dag

logger = logging.getLogger(__name__)

CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_BASE_DIR = CURRENT_DIR / "config"


def build_pg2adls_params(raw_params: dict) -> dict:
    if not raw_params:
        raise ValueError("❌ La section 'params' du YAML ne peut pas être vide.")

    required_keys = [
        "ID_PIPELINE", "GIT_CONN_ID", "POSTGRESQL_SOURCE", "USE_AZURITE",
        "AZURE_CONN_ID", "SCHEMA_SOURCE", "TABLES", "CONTENEUR_AZURE",
        "MOTEUR_DLT", "STRATEGIE_ECRITURE", "TAILLE_LOT"
    ]
    missing = [k for k in required_keys if k not in raw_params]
    if missing:
        raise KeyError(f"❌ Paramètres manquants dans le YAML : {missing}")

    # Récupération des valeurs YAML servant de valeurs par défaut dans l'UI Airflow
    default_moteur = str(raw_params["MOTEUR_DLT"]).lower()
    default_strategie = str(raw_params["STRATEGIE_ECRITURE"]).lower()

    # Alignement de l'option par défaut en tête de liste pour l'UI Airflow
    moteur_options = ["pyarrow", "connectorx"]
    if default_moteur in moteur_options:
        moteur_options.remove(default_moteur)
        moteur_options.insert(0, default_moteur)

    strategie_options = ["replace", "append", "merge"]
    if default_strategie in strategie_options:
        strategie_options.remove(default_strategie)
        strategie_options.insert(0, default_strategie)

    return {
        "ID_PIPELINE": Param(raw_params["ID_PIPELINE"], type="string"),
        "GIT_CONN_ID": Param(raw_params["GIT_CONN_ID"], type="string"),
        "POSTGRESQL_SOURCE": Param(raw_params["POSTGRESQL_SOURCE"], type="string"),
        "USE_AZURITE": Param(str(raw_params["USE_AZURITE"]).lower(), type="string", enum=["true", "false"]),
        "AZURE_CONN_ID": Param(raw_params["AZURE_CONN_ID"], type="string"),
        "SCHEMA_SOURCE": Param(raw_params["SCHEMA_SOURCE"], type="string"),
        
        "TABLES": Param(
            raw_params["TABLES"],
            type="array",
            description="Liste des tables et paramètres d'incrémentalité",
        ),
        
        "CONTENEUR_AZURE": Param(raw_params["CONTENEUR_AZURE"], type="string"),
        
        # Champs sous forme de menu déroulant (enum)
        "MOTEUR_DLT": Param(default_moteur, type="string", enum=moteur_options, description="Moteur d'extraction DLT"),
        "STRATEGIE_ECRITURE": Param(default_strategie, type="string", enum=strategie_options, description="Mode d'écriture globale sur ADLS"),
        
        "TAILLE_LOT": Param(raw_params["TAILLE_LOT"], type="integer"),
    }


def load_dags_for_typology(folder_name: str, build_params_fn, create_dag_fn):
    config_dir = CONFIG_BASE_DIR / folder_name
    if config_dir.exists():
        for yaml_file in config_dir.glob("*.yaml"):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                if cfg and "dag_id" in cfg:
                    dag_id = cfg["dag_id"]
                    params = build_params_fn(cfg.get("params"))
                    globals()[dag_id] = create_dag_fn(
                        dag_id=dag_id,
                        description=cfg.get("description", ""),
                        schedule=cfg.get("schedule"),
                        params=params,
                    )
            except Exception as e:
                logger.error(f"❌ Erreur lors du chargement de {folder_name}/{yaml_file.name}: {e}")
                raise e


load_dags_for_typology("postgres_adls", build_pg2adls_params, create_pg2adls_dag)