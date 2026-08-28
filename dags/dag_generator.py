import logging
import os
import yaml
import adlfs

from airflow import DAG
from airflow.sdk import Param

from factories.postgres_adls.kubernetes_mode_yaml import create_dag as create_pg2adls_dag

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 1. CONFIGURATION DU DEPOSITAIRE DE YAML (AZURITE / ADLS)
# -----------------------------------------------------------------------------
# On peut utiliser une variable d'environnement système ou la fixer par défaut
USE_AZURITE = os.environ.get("USE_AZURITE", "true").lower() in ("true", "1", "yes")

CONFIG_CONTAINER = "configs-dags"  # Le conteneur qui hébergera tes YAML

if USE_AZURITE:
    storage_options = {
        "account_name": "devstoreaccount1",
        "account_key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "custom_domain": "azurite:10000/devstoreaccount1",
        "use_ssl": False,
    }
else:
    # Mode Prod Azure : Utilise la Connection String ou les identifiants d'environnement
    storage_options = {
        "connection_string": os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    }


def build_pg2adls_params(cfg: dict) -> dict:
    if not cfg:
        raise ValueError("❌ Le fichier YAML ne peut pas être vide.")

    for section in ["infrastructure", "pipeline"]:
        if section not in cfg:
            raise KeyError(f"❌ Bloc obligatoire manquant dans le YAML : '{section}'")

    infra = cfg["infrastructure"]
    pipeline = cfg["pipeline"]

    tables = pipeline.get("tables", [])
    if not tables or not isinstance(tables, list):
        raise ValueError("❌ La section 'pipeline.tables' doit être une liste non vide.")

    for idx, tbl in enumerate(tables):
        for sub_block in ["source", "target", "execution"]:
            if sub_block not in tbl:
                raise KeyError(f"❌ La table #{idx + 1} doit obligatoirement contenir le bloc '{sub_block}'.")

        src = tbl["source"]
        tgt = tbl["target"]
        exe = tbl["execution"]

        required_src = ["schema", "table"]
        required_tgt = ["container", "dataset", "name", "format", "write_strategy", "use_partition"]
        required_exe = ["backend", "chunk_size"]

        missing_src = [k for k in required_src if k not in src]
        missing_tgt = [k for k in required_tgt if k not in tgt]
        missing_exe = [k for k in required_exe if k not in exe]

        if missing_src or missing_tgt or missing_exe:
            raise KeyError(
                f"❌ Propriétés manquantes dans la table #{idx + 1} ({src.get('table', 'inconnue')}) : "
                f"source={missing_src}, target={missing_tgt}, execution={missing_exe}"
            )

    return {
        "ID_PIPELINE": cfg.get("dag_id"),
        "GIT_CONN_ID": Param(infra["git_connection_id"], type="string"),
        "POSTGRESQL_SOURCE": Param(infra["postgres_connection_id"], type="string"),
        "AZURE_CONN_ID": Param(infra["azure_connection_id"], type="string"),
        "USE_AZURITE": Param(str(infra["use_azurite"]).lower(), type="string", enum=["true", "false"]),

        "TABLES": Param(tables, type="array", description="Liste des tables et de leurs configurations d'export"),
    }


def load_dags_from_blob_storage(typology_folder: str, build_params_fn, create_dag_fn):
    """
    Parcourt les fichiers YAML présents dans le Blob Storage / Azurite 
    sous le dossier 'configs-dags/<typology_folder>/*.yaml'
    """
    try:
        # Connexion au système de fichiers Azurite / Azure Blob
        fs = adlfs.AzureBlobFileSystem(**storage_options)
        
        path_pattern = f"{CONFIG_CONTAINER}/{typology_folder}/*.yaml"
        yaml_files = fs.glob(path_pattern)

        for remote_yaml_path in yaml_files:
            try:
                with fs.open(remote_yaml_path, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)

                if cfg and "dag_id" in cfg:
                    dag_id = cfg["dag_id"]
                    params = build_params_fn(cfg)

                    # Génération dynamique dans le contexte Airflow
                    globals()[dag_id] = create_dag_fn(
                        dag_id=dag_id,
                        description=cfg.get("description", ""),
                        schedule=cfg.get("schedule"),
                        params=params,
                    )
                    logger.info(f"✅ DAG {dag_id} chargé depuis Azurite/ADLS ({remote_yaml_path})")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du YAML {remote_yaml_path}: {e}")

    except Exception as e:
        logger.error(f"❌ Impossible d'accéder au conteneur de conf '{CONFIG_CONTAINER}' : {e}")


# Chargement dynamique des YAML stockés dans Azurite (dossier 'postgres_adls')
load_dags_from_blob_storage("postgres_adls", build_pg2adls_params, create_pg2adls_dag)