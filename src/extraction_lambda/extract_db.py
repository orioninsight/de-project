import os
import logging
import json
from extraction.extractor import Extractor
from extraction.saver import Saver
from extraction.storer import Storer
from secret_manager.retrieve_entry import retrieve_entry

""" The logging level is set to INFO, which means that only messages of
    level INFO will be logged.
"""
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def extract_db_handler(event, context):
    """ AWS Lambda to extract tables from Totesys DB

        Relies on environmental variable 'OI_TOTESYS_SECRET_STRING'
        or AWS secret called 'totesys_db' to be
        a parseable JSON string with the following key-value pairs:

        - user=<db user>
        - password=<db password>
        - host=<URL to host>
        - port=<port db is listening on>
        - database=<name of database>

        or as JSON: {"host":"HOST","port":"PORT","user":"USER","password":"PASSWORD","database":"DB"}

        Relies on environt variable 'OI_STORER_SECRET_STRING' or AWS secret called 'storer_info' to be
        a parseable JSON string with the following key-value pairs:

        - s3_bucket_name=<Name of the S3 bucket to store extracted files'

        or as JSON: {"s3_bucket_name":"BUCKET_NAME"}
    """  # noqa: E501

    try:
        if event is None or 'extract_table' not in event:
            raise Exception("Event payload requires list in 'extract_table'")
        db_secret_string = os.environ['OI_TOTESYS_SECRET_STRING']  \
            if 'OI_TOTESYS_SECRET_STRING' in \
            os.environ else retrieve_entry('totesys_db')
        db_secret_json = json.loads(db_secret_string)
        extractor = Extractor(**db_secret_json)
        saver = Saver()
        storer_secret_string = os.environ['OI_STORER_SECRET_STRING'] \
            if 'OI_STORER_SECRET_STRING' in \
            os.environ else retrieve_entry('storer_info')
        storer_secret_json = json.loads(storer_secret_string)
        storer = Storer(**storer_secret_json)

        if ((tables_to_extract := event['extract_table'])):
            for table in tables_to_extract:
                if table in ['address', 'design', 'counterparty',
                             'purchase_order', 'staff', 'sales_order',
                             'payment', 'transaction', 'payment_type',
                             'currency', 'department']:
                    extract_fn = getattr(extractor, f'extract_{table}')
                    data = extract_fn()
                    file_name = f'{table}.csv'
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

    except Exception as e:
        logger.error(f'An error occurred extracting the data: {e}')
        raise RuntimeError(e)
