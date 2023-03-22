
import pg8000.native
import logging
import boto3
from botocore.exceptions import ClientError
import json
from src.secret_manager.retrieve_entry import retrieve_entry
from src.extraction.extractor import Extractor

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def extract_db_handler(event, context):
    """ AWS Lambda to extract tables from Totesys DB

        Relies on existing AWS secret called 'totesys_db' to obtain DB credentials
    """
    try:
        secret_string = retrieve_entry('totesys_db')
        secret_json = json.loads(secret_string)
        extractor = Extractor(**secret_json)
        # print(extractor.extract_address())
    except Exception as e:
        logger.error(f'An error occurred extracting the data: {e}')
        raise RuntimeError


def get_db_credentials(secret_id):
    sm_secret = boto3.client("secretsmanager")
    secret_string = sm_secret.get_secret_value(
        SecretId=secret_id)["SecretString"]
    return json.loads(secret_string)


extract_db_handler(None, None)
