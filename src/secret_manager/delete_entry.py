import boto3


def delete_entry(secret_id):
    client = boto3.client('secretsmanager')

    try:
        client.delete_secret(
            SecretId=secret_id,
            ForceDeleteWithoutRecovery=True
        )
        print("Deleted")
    except client.exceptions.ResourceNotFoundException:
        print("Secret not found")
