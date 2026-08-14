import logging
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Param

# Importation des factories distinctes
from factories.postgres_postgres.kubernetes_mode_yaml import create_dag as create_pg2pg_dag
from factories.adls_postgres.kubernetes_mode_yaml import create_dag as create_adls2pg_dag
from factories.postgres_adls.kubernetes_mode_yaml import create_dag as create_pg2adls_dag

logger = logging.getLogger(__name__)

CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_BASE_DIR = CURRENT_DIR / "config"


# --- BUILDERS DE PARAMETRES PAR TYPOLOGIE ---

def build_pg2pg_params(raw_params: dict) -> dict:
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(raw_params.get("ID_PIPELINE", "DEFAULT_ID"), type="string"),
        "GIT_CONN_ID": Param(raw_params.get("GIT_CONN_ID", "git-dlt"), type="string"),
        "POSTGRESQL_SOURCE": Param(raw_params.get("POSTGRESQL_SOURCE", "postgres_source"), type="string"),
        "POSTGRESQL_CIBLE": Param(raw_params.get("POSTGRESQL_CIBLE", "postgres_cible"), type="string"),
        "SCHEMA_SOURCE": Param(raw_params.get("SCHEMA_SOURCE", "dlt"), type="string"),
        "TABLE_SOURCE": Param(raw_params.get("TABLE_SOURCE", "orders"), type="string"),
        "SCHEMA_CIBLE": Param(raw_params.get("SCHEMA_CIBLE", "dlt"), type="string"),
        "TABLE_CIBLE": Param(raw_params.get("TABLE_CIBLE", "orders"), type="string"),
        "STRATEGIE_ECRITURE": Param(default=raw_params.get("STRATEGIE_ECRITURE", "REPLACE"), type="string"),
        "CLE_PRIMAIRE": Param(raw_params.get("CLE_PRIMAIRE"), type=["array", "null"]),
        "MOTEUR_DLT": Param(default=raw_params.get("MOTEUR_DLT", "connectorx"), type="string"),
        "TAILLE_LOT": Param(raw_params.get("TAILLE_LOT", 100000), type="integer"),
    }


def build_adls2pg_params(raw_params: dict) -> dict:
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(raw_params.get("ID_PIPELINE", "DEFAULT_ADLS_ID"), type="string"),
        "GIT_CONN_ID": Param(raw_params.get("GIT_CONN_ID", "git-dlt"), type="string"),
        "USE_AZURITE": Param(raw_params.get("USE_AZURITE", "true"), type="string", enum=["true", "false"]),
        "AZURE_CONN_ID": Param(raw_params.get("AZURE_CONN_ID", "azure_storage_default"), type="string"),
        "POSTGRESQL_CIBLE": Param(raw_params.get("POSTGRESQL_CIBLE", "postgres_cible"), type="string"),
        "CONTENEUR_AZURE": Param(raw_params.get("CONTENEUR_AZURE", "source-data"), type="string"),
        # Accepte "*.parquet", "test.parquet,items.parquet" ou "users_*.parquet,orders.parquet"
        "PATTERN_FICHIER": Param(raw_params.get("PATTERN_FICHIER", "*.parquet"), type="string"),
        "SCHEMA_CIBLE": Param(raw_params.get("SCHEMA_CIBLE", "dlt"), type="string"),
        "TABLE_CIBLE": Param(raw_params.get("TABLE_CIBLE", "users_test"), type="string"),
        "STRATEGIE_ECRITURE": Param(default=raw_params.get("STRATEGIE_ECRITURE", "replace"), type="string"),
    }


def build_pg2adls_params(raw_params: dict) -> dict:
    raw_params = raw_params or {}
    return {
        "ID_PIPELINE": Param(raw_params.get("ID_PIPELINE", "DEFAULT_PG2ADLS_ID"), type="string"),
        "GIT_CONN_ID": Param(raw_params.get("GIT_CONN_ID", "git-dlt"), type="string"),
        "POSTGRESQL_SOURCE": Param(raw_params.get("POSTGRESQL_SOURCE", "postgres_source"), type="string"),
        "USE_AZURITE": Param(raw_params.get("USE_AZURITE", "true"), type="string", enum=["true", "false"]),
        "AZURE_CONN_ID": Param(raw_params.get("AZURE_CONN_ID", "azure_storage_default"), type="string"),
        "SCHEMA_SOURCE": Param(raw_params.get("SCHEMA_SOURCE", "public"), type="string"),
        # Accepte "orders", "orders,items,customers" ou "*"
        "TABLE_SOURCE": Param(raw_params.get("TABLE_SOURCE", "*"), type="string"),
        "CONTENEUR_AZURE": Param(raw_params.get("CONTENEUR_AZURE", "target-data"), type="string"),
        "MOTEUR_DLT": Param(default=raw_params.get("MOTEUR_DLT", "connectorx"), type="string"),
        "TAILLE_LOT": Param(raw_params.get("TAILLE_LOT", 100000), type="integer"),
    }


# --- SCAN AUTOMATIQUE PAR TYPOLOGIE ---

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

# Chargement dynamique des 3 typologies
load_dags_for_typology("postgres_postgres", build_pg2pg_params, create_pg2pg_dag)
load_dags_for_typology("adls_postgres", build_adls2pg_params, create_adls2pg_dag)
load_dags_for_typology("postgres_adls", build_pg2adls_params, create_pg2adls_dag)