import logging
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Param

# Importation des deux factories distinctes
from factories.postgres_postgres.kubernetes_mode_yaml import create_dag as create_pg2pg_dag
from factories.adls_postgres.kubernetes_mode_yaml import create_dag as create_adls2pg_dag

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
        "POSTGRESQL_CIBLE": Param(raw_params.get("POSTGRESQL_CIBLE", "postgres_target"), type="string"),
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
        "POSTGRESQL_CIBLE": Param(raw_params.get("POSTGRESQL_CIBLE", "postgres_target"), type="string"),
        "CONTENEUR_AZURE": Param(raw_params.get("CONTENEUR_AZURE", "source-data"), type="string"),
        "PATTERN_FICHIER": Param(raw_params.get("PATTERN_FICHIER", "*.parquet"), type="string"),
        "SCHEMA_CIBLE": Param(raw_params.get("SCHEMA_CIBLE", "dlt"), type="string"),
        "TABLE_CIBLE": Param(raw_params.get("TABLE_CIBLE", "users_test"), type="string"),
        "STRATEGIE_ECRITURE": Param(default=raw_params.get("STRATEGIE_ECRITURE", "replace"), type="string"),
    }


# --- SCAN AUTOMATIQUE PAR TYPOLOGIE ---

# 1. Typologie Postgres -> Postgres
PG_CONFIG_DIR = CONFIG_BASE_DIR / "postgres_postgres"
if PG_CONFIG_DIR.exists():
    for yaml_file in PG_CONFIG_DIR.glob("*.yaml"):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            if cfg and "dag_id" in cfg:
                dag_id = cfg["dag_id"]
                params = build_pg2pg_params(cfg.get("params", {}))
                globals()[dag_id] = create_pg2pg_dag(
                    dag_id=dag_id,
                    description=cfg.get("description", ""),
                    schedule=cfg.get("schedule"),
                    params=params,
                )
        except Exception as e:
            logger.error(f"Erreur chargement PG2PG {yaml_file.name}: {e}")

# 2. Typologie ADLS -> Postgres
ADLS_CONFIG_DIR = CONFIG_BASE_DIR / "adls_postgres"
if ADLS_CONFIG_DIR.exists():
    for yaml_file in ADLS_CONFIG_DIR.glob("*.yaml"):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            if cfg and "dag_id" in cfg:
                dag_id = cfg["dag_id"]
                params = build_adls2pg_params(cfg.get("params", {}))
                globals()[dag_id] = create_adls2pg_dag(
                    dag_id=dag_id,
                    description=cfg.get("description", ""),
                    schedule=cfg.get("schedule"),
                    params=params,
                )
        except Exception as e:
            logger.error(f"Erreur chargement ADLS2PG {yaml_file.name}: {e}")