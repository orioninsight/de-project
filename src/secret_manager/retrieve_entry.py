import boto3
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def retrieve_entry(secret_id):
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(
            SecretId=secret_id)
        secret = response.get('SecretString')
    except Exception as e:
        logger.error(f'An error occurred because: {e}')
        raise e

    return secret
