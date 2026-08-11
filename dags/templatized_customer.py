from airflow.models.param import Param
from lib.postgres_postgres_factory_kubernetes import create_dlt_postgres_dag

dag = create_dlt_postgres_dag(
    dag_id="templatized__postgres_postgres_CUSTOMERS",
    description="Incrémentation et mise à jour de la table clients",
    schedule="0 2 * * *",
    params={
        "ID_PIPELINE": Param(
            "CUSTOMERS_PIPELINE_MERGE",
            type="string",
            title="ID du Pipeline DLT"
        ),
        "POSTGRESQL_SOURCE": Param(
            "pg-source",
            type="string",
            title="Connexion Airflow Source"
        ),
        "POSTGRESQL_CIBLE": Param(
            "pg-target",
            type="string",
            title="Connexion Airflow Cible"
        ),
        "SCHEMA_SOURCE": Param(
            "public",
            type="string"
        ),
        "TABLE_SOURCE": Param(
            "customers",
            type="string"
        ),
        "SCHEMA_CIBLE": Param(
            "raw_data",
            type="string"
        ),
        "TABLE_CIBLE": Param(
            "customers",
            type="string"
        ),
        "STRATEGIE_ECRITURE": Param(
            default="METTRE_A_JOUR",
            type="string",
            enum=["ECRASER", "AJOUTER", "METTRE_A_JOUR"],
            title="Stratégie d'écriture",
            description="Choisissez la stratégie d'écriture des données."
        ),
        "CLE_PRIMAIRE": Param(
            ["customer_id"],
            type=["array", "null"],
            title="Clé(s) primaire(s) pour le MERGE"
        ),
        "MOTEUR_DLT": Param(
            default="connectorx",
            type="string",
            enum=["connectorx", "pyarrow", "pandas"],
            title="Moteur d'extraction DLT",
        ),
        "TAILLE_LOT": Param(
            100000,
            type="integer",
            title="Taille des lots (chunk size)"
        ),
    }
)