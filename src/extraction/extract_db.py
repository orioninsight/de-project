
import pg8000.native
import logging
import boto3
from botocore.exceptions import ClientError
import json

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def extract_db_handler(event, context):
    # create connection and get credentials
    # extract tables and store as files
    pass


def get_db_credentials(secret_id):
    sm_secret = boto3.client("secretsmanager")
    secret_string = sm_secret.get_secret_value(
        SecretId=secret_id)["SecretString"]
    return json.loads(secret_string)
