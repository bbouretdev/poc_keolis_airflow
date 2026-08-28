import logging
import os
import yaml
from azure.storage.blob import BlobServiceClient

from airflow import DAG
from airflow.sdk import Param

from factories.postgres_adls.kubernetes_mode_yaml import create_dag as create_pg2adls_dag

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 1. CONFIGURATION DU DÉPOSITAIRE DE YAML (AZURITE / ADLS)
# -----------------------------------------------------------------------------
USE_AZURITE = os.environ.get("USE_AZURITE", "true").lower() in ("true", "1", "yes")
CONFIG_CONTAINER = "configs-dags"

# Nettoyage des variables : on force le nom DNS Kubernetes ou l'IP pure sans "tcp://"
AZURITE_HOST = os.environ.get("AZURITE_CUSTOM_HOST", "azurite")
AZURITE_PORT = "10000"  # Port HTTP pur

AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)


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


def fetch_yaml_configs_azurite(typology_folder: str) -> list[tuple[str, dict]]:
    """Lecture des YAML depuis Azurite via la Connection String explicite."""
    configs = []
    
    # URL construite proprement : http://azurite:10000/devstoreaccount1
    blob_endpoint = f"http://{AZURITE_HOST}:{AZURITE_PORT}/{AZURITE_ACCOUNT_NAME}"
    
    connection_string = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={AZURITE_ACCOUNT_NAME};"
        f"AccountKey={AZURITE_ACCOUNT_KEY};"
        f"BlobEndpoint={blob_endpoint};"
    )

    try:
        # Timeout très court (3s) pour éviter de bloquer l'import d'Airflow si Azurite est indisponible
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string,
            connection_timeout=3,
            read_timeout=3
        )
        container_client = blob_service_client.get_container_client(CONFIG_CONTAINER)

        prefix = f"{typology_folder}/"
        blobs = container_client.list_blobs(name_starts_with=prefix)

        for blob in blobs:
            if blob.name.endswith(".yaml") or blob.name.endswith(".yml"):
                blob_client = container_client.get_blob_client(blob.name)
                yaml_content = blob_client.download_blob().readall().decode("utf-8")
                cfg = yaml.safe_load(yaml_content)
                if cfg:
                    configs.append((blob.name, cfg))
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des YAML depuis Azurite ({CONFIG_CONTAINER}) : {e}")

    return configs


def fetch_yaml_configs_prod(typology_folder: str) -> list[tuple[str, dict]]:
    """Lecture des YAML depuis Azure ADLS Réel."""
    import adlfs
    configs = []
    try:
        fs = adlfs.AzureBlobFileSystem(
            connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        )
        path_pattern = f"{CONFIG_CONTAINER}/{typology_folder}/*.yaml"
        yaml_files = fs.glob(path_pattern)

        for remote_yaml_path in yaml_files:
            with fs.open(remote_yaml_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                if cfg:
                    configs.append((remote_yaml_path, cfg))
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération des YAML depuis Azure ADLS : {e}")

    return configs


def load_dags_from_blob_storage(typology_folder: str, build_params_fn, create_dag_fn):
    if USE_AZURITE:
        yaml_configs = fetch_yaml_configs_azurite(typology_folder)
    else:
        yaml_configs = fetch_yaml_configs_prod(typology_folder)

    for source_path, cfg in yaml_configs:
        try:
            if cfg and "dag_id" in cfg:
                dag_id = cfg["dag_id"]
                params = build_params_fn(cfg)

                globals()[dag_id] = create_dag_fn(
                    dag_id=dag_id,
                    description=cfg.get("description", ""),
                    schedule=cfg.get("schedule"),
                    params=params,
                )
                logger.info(f"✅ DAG '{dag_id}' généré avec succès depuis {source_path}")
        except Exception as e:
            logger.error(f"❌ Erreur lors du parsing du fichier {source_path} : {e}")


# Chargement dynamique
load_dags_from_blob_storage("postgres_adls", build_pg2adls_params, create_pg2adls_dag)