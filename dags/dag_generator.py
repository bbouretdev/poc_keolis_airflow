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
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(raw_params.get("ID_PIPELINE"), type="string"),
        "GIT_CONN_ID": Param(raw_params.get("GIT_CONN_ID"), type="string"),
        "POSTGRESQL_SOURCE": Param(raw_params.get("POSTGRESQL_SOURCE"), type="string"),
        "USE_AZURITE": Param(raw_params.get("USE_AZURITE"), type="string", enum=["true", "false"]),
        "AZURE_CONN_ID": Param(raw_params.get("AZURE_CONN_ID"), type="string"),
        "SCHEMA_SOURCE": Param(raw_params.get("SCHEMA_SOURCE"), type="string"),
        
        "TABLES": Param(
            raw_params.get("TABLES", []),
            type=["array", "null"],
            description="Liste des tables et alias de destination",
        ),
        
        "CONTENEUR_AZURE": Param(raw_params.get("CONTENEUR_AZURE"), type="string"),
        "MOTEUR_DLT": Param(default=raw_params.get("MOTEUR_DLT", "pyarrow"), type="string"),
        "STRATEGIE_ECRITURE": Param(default=raw_params.get("STRATEGIE_ECRITURE", "replace"), type="string"),
        "TAILLE_LOT": Param(raw_params.get("TAILLE_LOT", 100000), type="integer"),
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
                    params = build_params_fn(cfg.get("params", {}))
                    globals()[dag_id] = create_dag_fn(
                        dag_id=dag_id,
                        description=cfg.get("description", ""),
                        schedule=cfg.get("schedule"),
                        params=params,
                    )
            except Exception as e:
                logger.error(f"Erreur lors du chargement de {folder_name}/{yaml_file.name}: {e}")


load_dags_for_typology("postgres_adls", build_pg2adls_params, create_pg2adls_dag)