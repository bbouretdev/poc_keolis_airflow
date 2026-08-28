import logging
from pathlib import Path
import yaml

from airflow import DAG
from airflow.sdk import Param

from factories.postgres_adls.kubernetes_mode_yaml import create_dag as create_pg2adls_dag

logger = logging.getLogger(__name__)

CURRENT_DIR = Path(__file__).resolve().parent
CONFIG_BASE_DIR = CURRENT_DIR / "config"


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

    # Validation du découpage source / target / execution par table
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


def load_dags_for_typology(folder_name: str, build_params_fn, create_dag_fn):
    config_dir = CONFIG_BASE_DIR / folder_name
    if config_dir.exists():
        for yaml_file in config_dir.glob("*.yaml"):
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)

                if cfg and "dag_id" in cfg:
                    dag_id = cfg["dag_id"]
                    params = build_params_fn(cfg)

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