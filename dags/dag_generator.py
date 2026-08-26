import logging
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Param

# Import du constructeur de DAG Kubernetes/DLT
from factories.postgres_adls.kubernetes_mode_yaml import create_dag as create_pg2adls_dag

logger = logging.getLogger(__name__)

CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_BASE_DIR = CURRENT_DIR / "config"


def build_pg2adls_params(raw_params: dict) -> dict:
    """Valide la présence des paramètres YAML et les convertit en objets Param Airflow."""
    if not raw_params:
        raise ValueError("❌ La section 'params' du YAML ne peut pas être vide.")

    required_keys = [
        "ID_PIPELINE", "GIT_CONN_ID", "POSTGRESQL_SOURCE", "USE_AZURITE",
        "AZURE_CONN_ID", "SCHEMA_SOURCE", "DATASET_NAME", "TABLES",
        "CONTENEUR_AZURE", "MOTEUR_DLT", "STRATEGIE_ECRITURE", "TAILLE_LOT",
        "FORMAT_STOCKAGE"
    ]
    missing = [k for k in required_keys if k not in raw_params]
    if missing:
        raise KeyError(f"❌ Paramètres manquants dans le YAML : {missing}")

    return {
        "ID_PIPELINE": Param(raw_params["ID_PIPELINE"], type="string"),
        "GIT_CONN_ID": Param(raw_params["GIT_CONN_ID"], type="string"),
        "POSTGRESQL_SOURCE": Param(raw_params["POSTGRESQL_SOURCE"], type="string"),
        "USE_AZURITE": Param(str(raw_params["USE_AZURITE"]).lower(), type="string", enum=["true", "false"]),
        "AZURE_CONN_ID": Param(raw_params["AZURE_CONN_ID"], type="string"),
        "SCHEMA_SOURCE": Param(raw_params["SCHEMA_SOURCE"], type="string"),
        "DATASET_NAME": Param(raw_params["DATASET_NAME"], type="string"),
        "TABLES": Param(raw_params["TABLES"], type="array", description="Liste des objets {source, target_name, partition_col}"),
        "CONTENEUR_AZURE": Param(raw_params["CONTENEUR_AZURE"], type="string"),
        "MOTEUR_DLT": Param(raw_params["MOTEUR_DLT"], type="string"),
        "STRATEGIE_ECRITURE": Param(raw_params["STRATEGIE_ECRITURE"], type="string"),
        "TAILLE_LOT": Param(raw_params["TAILLE_LOT"], type="integer"),
        "FORMAT_STOCKAGE": Param(str(raw_params["FORMAT_STOCKAGE"]).lower(), type="string", enum=["delta", "parquet"]),
    }


def load_dags_for_typology(folder_name: str, build_params_fn, create_dag_fn):
    """Parcourt les fichiers YAML d'un sous-dossier de configuration et enregistre les DAGs dans globals()."""
    config_dir = CONFIG_BASE_DIR / folder_name
    if config_dir.exists():
        for yaml_file in config_dir.glob("*.yaml"):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)

                if cfg and "dag_id" in cfg:
                    dag_id = cfg["dag_id"]
                    params = build_params_fn(cfg.get("params"))

                    # Enregistrement dans le contexte global pour qu'Airflow détecte le DAG
                    globals()[dag_id] = create_dag_fn(
                        dag_id=dag_id,
                        description=cfg.get("description", ""),
                        schedule=cfg.get("schedule"),
                        params=params,
                    )
            except Exception as e:
                logger.error(f"❌ Erreur lors du chargement de {folder_name}/{yaml_file.name}: {e}")
                raise e


# Chargement des DAGs pour la typologie Postgres -> ADLS
load_dags_for_typology("postgres_adls", build_pg2adls_params, create_pg2adls_dag)