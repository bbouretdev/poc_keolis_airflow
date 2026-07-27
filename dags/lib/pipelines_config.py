# Defines the default git branch used to clone the DLT git repository
GIT_BRANCH = "main"

# Defines the local directory used to clone the DLT git repository
WORKING_DIR = "/tmp/dlt_git_repo"

# Defines the mapping of pipelines by source/destination technology and write strategy
PIPELINES_PATH = {
    "postgres_postgres": {
        "ECRASER": "pipelines/postgres_postgres/replace.py",
        "AJOUTER": "pipelines/postgres_postgres/append.py",
        "METTRE_A_JOUR": "pipelines/postgres_postgres/merge.py",
    },

    "postgres_adls": {
        "ECRASER": "pipelines/postgres_adls/replace.py",
        "AJOUTER": "pipelines/postgres_adls/append.py",
        "METTRE_A_JOUR": "pipelines/postgres_adls/merge.py",
    },

    "api_filesystem": {
        "ECRASER": "pipelines/api_filesystem/replace.py",
        "AJOUTER": "pipelines/api_filesystem/append.py",
        "METTRE_A_JOUR": "pipelines/api_filesystem/merge.py",
    },
}