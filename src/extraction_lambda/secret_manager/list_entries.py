import boto3


def list_secrets():
    client = boto3.client('secretsmanager')
    response = client.list_secrets(
        IncludePlannedDeletion=False,
        SortOrder='asc'
    )
    secrets_list = []
    for secret in response["SecretList"]:
        secrets_list.append(secret["Name"])
    return secrets_list
