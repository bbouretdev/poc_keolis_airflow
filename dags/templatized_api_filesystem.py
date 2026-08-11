from airflow.sdk import Param

from factories.api_filesystem_factory import create_dlt_dag


dag = create_dlt_dag(

    dag_id="templatized__opendata_rest_api__POKEMON",

    description="Extraction PokeAPI vers filesystem local",

    schedule="0 5 * * *",

    params={

        "ID_PIPELINE": Param(
            "POKEMON_PIPELINE",
            type="string"
        ),

        "API_BASE_URL": Param(
            "https://pokeapi.co/api/v2/",
            type="string"
        ),

        "ENDPOINT": Param(
            "pokemon",
            type="string"
        ),

        "API_KEY": Param(
            default=None,
            type=["string", "null"],
            title="Connexion Airflow pour la clé API",
            description="Nom d'une Connection Airflow contenant l'API key en 'password'. Laisser vide si l'API est publique."
        ),

        "CHEMIN_CIBLE": Param(
            "test_api",
            type="string"
        ),

        "TABLE_CIBLE": Param(
            "pokemon",
            type="string"
        ),

        "FORMAT_FICHIER": Param(
            default="jsonl",
            type="string",
            enum=["jsonl", "parquet", "csv"],
            title="Format de fichier",
        ),

        "STRATEGIE_ECRITURE": Param(
            default="ECRASER",
            type="string",
            enum=["ECRASER", "AJOUTER"],
            title="Stratégie d'écriture",
        ),

        "TAILLE_LOT": Param(
            100,
            type="integer"
        ),
    }
)