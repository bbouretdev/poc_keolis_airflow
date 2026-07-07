from airflow import DAG
from airflow.sdk import task, Param

from datetime import datetime

import os
import shutil
import subprocess
import sys

import json

from airflow.hooks.base import BaseHook

from config_dlt import (
    GIT_BRANCH,
    WORKING_DIR,
    PIPELINES_PATH,
)


with DAG(
    dag_id="POSTGRESQL_to_POSTGRESQL",
    description="Déclenche un flux d'ingestion de Postgresql vers Postgresql",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,

    params={

        "ID_PIPELINE": Param(
            default="PG_PG_PIPELINE_DE_TEST",
            type="string",
            description_md="""Identifiant du pipeline DLT sous-jacent\n
            Cette valeur sert de référence pour les mécanismes de rejeu natifs DLT"""
        ),

        "POSTGRESQL_SOURCE": Param(
            "pg-source",
            type="string",
            enum=["pg-source"],
            description="Identifiant de la connexion Airflow de la base de données source"
        ),

        "POSTGRESQL_CIBLE": Param(
            "pg-dest",
            type="string",
            enum=["pg-dest"],
            description="Identifiant de la connexion Airflow de la base de données cible"
        ),

        "SCHEMA_SOURCE": Param(
            default="dlt",
            type="string",
            description="Nom du schema Postgresql source"
        ),

        "TABLE_SOURCE": Param(
            default="orders",
            type="string",
            description="Nom de la table Postgresql source"
        ),

        "SCHEMA_CIBLE": Param(
            default="dlt",
            type="string",
            description="Nom du schema Postgresql cible"
        ),

        "TABLE_CIBLE": Param(
            default="orders",
            type="string",
            description="Nom de la table Postgresql cible"
        ),

        "STRATEGIE_ECRITURE": Param(
            "ECRASER",
            type="string",
            enum=["ECRASER", "AJOUTER", "METTRE_A_JOUR"],
            description="Stratégie d'écriture appliquée aux données en destination"
        ),

        "CLE_PRIMAIRE": Param(
            default=["colonne1", "colonne2", "colonne3"],
            type=["array", "null"],
            items={"type": "string"},
            description_md="""Liste des colonnes formant la clé primaire\n
            N'est utilisé que si STRATEGIE_ECRITURE = METTRE_A_JOUR"""
        ),

        "MOTEUR_DLT": Param(
            "connectorx",
            type="string",
            enum=["connectorx"],
            description="Moteur d'ingestion DLT"
        ),

        "TAILLE_LOT": Param(
            default=50000,
            type="integer",
            description="Nombre de lignes par lot de traitement"
        ),

    },

) as dag:


    @task
    def clone_repo(**context):

        git_host = BaseHook.get_connection("git-dlt").host
        print(f"Cloning {git_host}")

        if os.path.exists(WORKING_DIR):
            shutil.rmtree(WORKING_DIR)

        subprocess.run(
            [
                "git",
                "clone",
                "--branch",
                GIT_BRANCH,
                git_host,
                WORKING_DIR,
            ],
            check=True,
        )

        return WORKING_DIR

    @task
    def install_requirements(repo_path: str):

        requirements = os.path.join(
            repo_path,
            "requirements.txt"
        )

        if not os.path.exists(requirements):

            print(
                "No requirements.txt found"
            )

            return

        print(
            "Installing dependencies"
        )


        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-r",
                requirements
            ],
            check=True
        )

    @task
    def build_dlt_environment(**context):

        params = context["params"]

        source_conn = BaseHook.get_connection(params["POSTGRESQL_SOURCE"])
        dest_conn = BaseHook.get_connection(params["POSTGRESQL_CIBLE"])

        env = {
            # dlt runtime
            "RUNTIME__LOG_LEVEL": "DEBUG",
            "RUNTIME__DLTHUB_TELEMETRY": "false",
            "RUNTIME__WORKERS": "4",

            # Source database
            "SOURCES__SQL_DATABASE__CREDENTIALS__DRIVERNAME": "postgresql",
            "SOURCES__SQL_DATABASE__CREDENTIALS__DATABASE": source_conn.schema,
            "SOURCES__SQL_DATABASE__CREDENTIALS__USERNAME": source_conn.login,
            "SOURCES__SQL_DATABASE__CREDENTIALS__PASSWORD": source_conn.password,
            "SOURCES__SQL_DATABASE__CREDENTIALS__HOST": source_conn.host,
            "SOURCES__SQL_DATABASE__CREDENTIALS__PORT": str(source_conn.port),

            # Destination database
            "DESTINATION__POSTGRES_DEST__DESTINATION_TYPE": "postgres",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DRIVERNAME": "postgresql",
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__DATABASE": dest_conn.schema,
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__USERNAME": dest_conn.login,
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PASSWORD": dest_conn.password,
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__HOST": dest_conn.host,
            "DESTINATION__POSTGRES_DEST__CREDENTIALS__PORT": str(dest_conn.port),
        }

        env["DLT_PIPELINE_ID"] = params["ID_PIPELINE"]
        env["DLT_SOURCE_TABLE"] = params["TABLE_SOURCE"]
        env["DLT_SOURCE_SCHEMA"] = params["SCHEMA_SOURCE"]
        env["DLT_TARGET_TABLE"] = params["TABLE_CIBLE"]
        env["DLT_TARGET_SCHEMA"] = params["SCHEMA_CIBLE"]
        env["DLT_BACKEND"] = params["MOTEUR_DLT"]
        env["DLT_CHUNK_SIZE"] = str(params["TAILLE_LOT"])
        if params["CLE_PRIMAIRE"] is not None:
            env["DLT_PRIMARY_KEY"] = json.dumps(params["CLE_PRIMAIRE"])

        print(
            "Generated DLT environment:"
        )

        for key in env:
            print(key)

        return env

    @task
    def run_pipeline(
        repo_path: str,
        dlt_env: dict,
        **context
    ):

        params = context["params"]

        pipeline = os.path.join(
            repo_path,
            PIPELINES_PATH[params["STRATEGIE_ECRITURE"]]
        )

        print(
            f"Executing pipeline {pipeline}"
        )

        env = os.environ.copy()
        env.update(dlt_env)
        env["PYTHONPATH"] = repo_path

        subprocess.run(
            [
                sys.executable,
                pipeline
            ],
            check=True,
            env=env,
            cwd=os.path.dirname(pipeline)
        )



    repo = clone_repo()

    deps = install_requirements(repo)

    runtime_env = build_dlt_environment()


    execution = run_pipeline(
        repo,
        runtime_env
    )


    repo >> deps >> runtime_env >> execution