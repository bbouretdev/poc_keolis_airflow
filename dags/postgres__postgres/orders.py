from airflow.models.param import Param
from factories.postgres_postgres.kubernetes_mode import create_dag

dag = create_dlt_postgres_dag(
    dag_id="orders",
    description="Incrémentation et mise à jour de la table commandes",
    schedule="0 2 * * *",
    params={
        "ID_PIPELINE": Param(
            "ORDERS",
            type="string",
            title="ID du Pipeline DLT"
        ),
        "POSTGRESQL_SOURCE": Param(
            "postgres_source",
            type="string",
            title="Connexion Airflow Source"
        ),
        "POSTGRESQL_CIBLE": Param(
            "postgres_cible",
            type="string",
            title="Connexion Airflow Cible"
        ),
        "SCHEMA_SOURCE": Param(
            "dlt",
            type="string"
        ),
        "TABLE_SOURCE": Param(
            "orders",
            type="string"
        ),
        "SCHEMA_CIBLE": Param(
            "dlt",
            type="string"
        ),
        "TABLE_CIBLE": Param(
            "orders",
            type="string"
        ),
        "STRATEGIE_ECRITURE": Param(
            default="REPLACE",
            type="string",
            enum=["REPLACE", "APPEND", "UPDATE"],
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