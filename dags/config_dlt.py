# Defines the default git branch used to clone the DLT git repository
GIT_BRANCH = "main"

# Defines the local directory used to clone the DLT git repository
WORKING_DIR = "/tmp/dlt_git_repo"

# Defines the mapping, routing to a pipeline depending on the selected write strategy
PIPELINES_PATH = {
    "ECRASER": "pipelines/postgres_postgres/replace.py",
    "AJOUTER": "pipelines/postgres_postgres/append.py",
    "METTRE_A_JOUR": "pipelines/postgres_postgres/merge.py",
}