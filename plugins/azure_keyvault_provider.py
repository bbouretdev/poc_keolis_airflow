import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class AzureKeyVaultProvider:
    def __init__(self):
        vault_url = os.getenv("AZURE_KEYVAULT_URL")
        credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=vault_url, credential=credential)

    def __getitem__(self, key: str):
        # mapping dlt -> KV naming
        secret_name = key.replace(".", "--").replace("_", "-")
        return self.client.get_secret(secret_name).value