# shared_functions/secret_manager.py

from google.cloud import secretmanager


def access_secret_version(
    secret_id: str, project_id: str = "shabubsinc", version_id: str = "latest"
) -> str:
    """
    Access the payload for the given secret version.

    Args:
        secret_id (str): The ID of the secret in Secret Manager.
        project_id (str): The Google Cloud Project ID where the secret is stored.
        version_id (str): The version of the secret to access. Defaults to "latest".

    Returns:
        str: The secret payload as a string.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
