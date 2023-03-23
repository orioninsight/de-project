import boto3

import botocore
import pytest
from moto import mock_secretsmanager
import json
import os

from botocore.exceptions import ClientError


def retrieve_entry(secret_id):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(
        SecretId=secret_id)
    try:
        secret = response.get('SecretString')
        with open("secrets.txt", "w") as f:
            # Writing data to a file
            f.write(secret)
        print("Secrets stored in local file secrets.txt")
        return secret
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_id + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print(
                "The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
