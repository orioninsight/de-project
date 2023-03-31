import os
import logging
import json
import boto3
from extraction.extractor import Extractor
from extraction.saver import Saver
from extraction.storer import Storer
from secret_manager.retrieve_entry import retrieve_entry
from extraction.monitor import Monitor

""" The logging level is set to INFO, which means that only messages of
    level INFO will be logged.
"""
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

VALID_TABLES = ['address', 'design', 'counterparty',
                'purchase_order', 'staff', 'sales_order',
                'payment', 'transaction', 'payment_type',
                'currency', 'department']

extractor = None
saver = None
storer = None


def extract_db_handler(event, context):
    global extractor, saver, storer
    try:
        tables_to_extract = []
        if event is None or event == {} or 'extract_table' not in event:
            tables_to_extract = VALID_TABLES
        else:
            tables_to_extract = event['extract_table']
        if type(tables_to_extract) is not list:
            raise (Exception("Event payload requires list "
                             f"in 'extract_table' but got {event} instead"))
        db_secret_json = load_env_var('OI_TOTESYS_DB_INFO',
                                      ['host', 'port', 'user',
                                       'password', 'database'], True)
        storer_info_json = load_env_var('OI_STORER_INFO', ['s3_bucket_name'])
        transform_lambda_info_json = load_env_var('OI_TRANSFORM_LAMBDA_INFO',
                                                  ['transform_lambda_arn'])
        extractor = Extractor(**db_secret_json)
        saver = Saver()
        storer = Storer(**storer_info_json)
        logger.info(storer_info_json["s3_bucket_name"])
        monitor = Monitor(storer_info_json["s3_bucket_name"], extractor)
        logger.info('Checking state of db...')
        if monitor.has_state_changed():
            extract_db_helper(tables_to_extract)
            call_transform_lambda(
                transform_lambda_info_json['transform_lambda_arn'],
                event, context)
    except Exception as e:
        logger.error(f'An error occurred extracting the data: {e}')
        raise e
    finally:
        if 'extractor' in globals() and extractor is not None:
            extractor.close()


def extract_db_helper(tables_to_extract):
    """ AWS Lambda to extract tables from Totesys DB

        Relies on environmental variable 'OI_TOTESYS_DB_INFO'
        or AWS secret called 'totesys_db' to be
        a parseable JSON string with the following key-value pairs:

        - user=<db user>
        - password=<db password>
        - host=<URL to host>
        - port=<port db is listening on>
        - database=<name of database>

        or as JSON: {"host":"HOST","port":"PORT","user":"USER","password":"PASSWORD","database":"DB"}

        Relies on environt variable 'OI_STORER_INFO' or AWS secret called 'storer_info' to be
        a parseable JSON string with the following key-value pairs:

        - s3_bucket_name=<Name of the S3 bucket to store extracted files'

        or as JSON: {"s3_bucket_name":"BUCKET_NAME"}
    """  # noqa: E501
    for table in tables_to_extract:
        if table in VALID_TABLES:
            extract_fn = getattr(extractor, f'extract_{table}')
            data = extract_fn()
            file_name = f'/tmp/{table}.csv'
            if not saver.save_data(data, file_name):
                raise Exception(f"Could not save table '{table}' data")
            logger.info(
                f'Data from table {table} saved to file {file_name}')
            if not storer.store_file(file_name, table):
                raise Exception(
                    f"""Could not store data file
                        '{file_name}' of table '{table}'""")
            logger.info(f'Data from table {table} stored on S3')
        else:
            raise Exception(f"Unsupported table '{table}' to extract")


def call_transform_lambda(fnArn, event, context):
    client = boto3.client('lambda')
    inputParams = {}
    response = client.invoke(
        FunctionName=fnArn,
        InvocationType='RequestResponse',
        Payload=json.dumps(inputParams)
    )
    logger.info('Invoking transform lambda...')
    res = json.load(response['Payload'])
    logger.info(f'Tranform lambda responded with {res}')


def load_env_var(env_key, expected_json_keys, is_secret=False):
    try:
        if env_key in os.environ:
            env_string = os.environ[env_key]
        elif is_secret:
            env_string = retrieve_entry(env_key)
        env_json = json.loads(env_string)
        for key in expected_json_keys:
            if key not in env_json:
                raise Exception(f'Missing key in env var ({env_key}): ({key})')
        return env_json
    except Exception:
        raise Exception(f'Error loading JSON for env var ({env_key})')
