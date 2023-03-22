import boto3


def create_entry(secret_id, host, port, user, password, database):
    client = boto3.client('secretsmanager')
    client.create_secret(
        Name=secret_id,
        SecretString=f'{{"host":"{host}","port":"{port}","user":"{user}",\
        "password":"{password}","database":"{database}"}}'
    )

    return "Secret saved."
