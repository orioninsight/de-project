import boto3
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def create_entry(secret_id, host, port, user, password, database):
    client = boto3.client('secretsmanager')
    try:
        client.create_secret(
            Name=secret_id,
            SecretString=f'{{"host":"{host}","port":"{port}","user":"{user}",\
            "password":"{password}","database":"{database}"}}'
        )
    except Exception as error:
        logger.error(f'An error occurred because: {error}')
        raise error

    return "Secret saved."
