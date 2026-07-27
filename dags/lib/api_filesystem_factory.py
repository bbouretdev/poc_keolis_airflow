from airflow import DAG
from airflow.sdk import task

from datetime import datetime

import os
import shutil
import subprocess
import sys
import json

from airflow.hooks.base import BaseHook

from lib.pipelines_config import (
    GIT_BRANCH,
    WORKING_DIR,
    PIPELINES_PATH,
)


def create_dlt_dag(
    dag_id: str,
    description: str,
    params: dict,
    schedule=None,
):

    with DAG(
        dag_id=dag_id,
        description=description,
        start_date=datetime(2024, 1, 1),
        schedule=schedule,
        catchup=False,
        params=params,
    ) as dag:


        @task
        def clone_repo():

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
                print("No requirements.txt found")
                return

            print("Installing dependencies")

            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "-r",
                    requirements
                ],
                check=True,
            )



        @task
        def build_dlt_environment(**context):

            params = context["params"]

            env = {

                # dlt runtime
                "RUNTIME__LOG_LEVEL": "DEBUG",
                "RUNTIME__DLTHUB_TELEMETRY": "false",
                "RUNTIME__WORKERS": "4",
                "DESTINATION__FILESYSTEM__BUCKET_URL": "file:///opt/airflow/dlt_output",
            }

            env["DLT_PIPELINE_ID"] = params["ID_PIPELINE"]

            # Source REST API
            env["DLT_SOURCE_API_BASE_URL"] = params["API_BASE_URL"]
            env["DLT_SOURCE_ENDPOINT"] = params["ENDPOINT"]

            if params.get("API_KEY") is not None:
                # Récupéré via une Connection Airflow plutôt qu'en clair dans les Params
                api_conn = BaseHook.get_connection(params["API_KEY"])
                env["DLT_SOURCE_API_KEY"] = api_conn.password

            # Destination filesystem
            env["DLT_TARGET_PATH"] = params["CHEMIN_CIBLE"]
            env["DLT_TARGET_FILENAME"] = params["TABLE_CIBLE"]
            env["DLT_FILE_FORMAT"] = params["FORMAT_FICHIER"]

            env["DLT_CHUNK_SIZE"] = str(params["TAILLE_LOT"])

            print("Generated DLT environment:")
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
                PIPELINES_PATH["api_filesystem"][
                    params["STRATEGIE_ECRITURE"]
                ]
            )

            print(f"Executing pipeline {pipeline}")

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
                cwd=os.path.dirname(pipeline),
                stderr=subprocess.STDOUT,
            )


        repo = clone_repo()
        deps = install_requirements(repo)
        runtime_env = build_dlt_environment()

        execution = run_pipeline(
            repo,
            runtime_env,
        )

        repo >> deps >> runtime_env >> execution


    return dag