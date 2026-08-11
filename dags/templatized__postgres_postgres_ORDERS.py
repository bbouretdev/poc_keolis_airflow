from airflow.sdk import Param

from factories.postgres_postgres_factory import create_dlt_dag


dag = create_dlt_dag(

    dag_id="templatized__postgres_postgres_ORDERS",

    description="Chargement des commandes",
    schedule="0 5 * * *",
    params={

        "ID_PIPELINE": Param(
            "ORDERS_PIPELINE_REPLACE",
            type="string"
        ),

        "POSTGRESQL_SOURCE": Param(
            "pg-source",
            type="string"
        ),

        "POSTGRESQL_CIBLE": Param(
            "pg-target",
            type="string"
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
            default="ECRASER",
            type="string",
            enum=["ECRASER", "AJOUTER", "METTRE_A_JOUR"],
            title="Stratégie d'écriture",
            description="Choisissez la stratégie d'écriture des données."
        ),

        "CLE_PRIMAIRE": Param(
            ["order_number"],
            type=["array", "null"]
        ),

        "MOTEUR_DLT": Param(
            default="connectorx",
            type="string",
            enum=["connectorx", "pyarrow", "pandas"],
            title="Moteur d'exécution DLT",
        ),

        "TAILLE_LOT": Param(
            50000,
            type="integer"
        ),
    }
)