import logging


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def load_lambda_handler(event, context):
    loader = Loader()
    loader.load()


class Loader:

    def __init__(self):
        pass
import pandas as pd
import boto3
import logging
import os
import json
import pg8000.native
from io import BytesIO
from sqlalchemy import URL
from sqlalchemy import create_engine,text


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def loader_handler(event, context):
    loader = Loader()
    for table in loader.TABLE_LIST:
        loader.delete_table(table)
    
    

class Loader:

    TABLE_LIST = ['fact_sales_order', 'dim_currency', 'dim_counterparty',
                  'dim_staff', 'dim_design', 'dim_date', 'dim_location']

    # FILE_LIST = ['address': 'dim_location', 'design':'dim_design', 'counterparty',
    #              'staff', 'sales_order',
    #              'currency', 'date']
    
    conn = None
    engine = None
    def __init__(self):
        self.s3_client = boto3.client('s3')
        # self.s3_processed_bucket_name = processed_bucket_name
        self.database = 'test'  # database

        db_secret_json = self.load_env_var('OI_TOTESYS_DW_INFO',
                                           ['host', 'port', 'user',
                                            'password', 'database'], False)
        self.s3_processed_bucket_name = self.load_env_var(
            'OI_PROCESSED_INFO', ['s3_bucket_name'])['s3_bucket_name']
        # self.connect_db(**db_secret_json)

    def load_env_var(self, env_key, expected_json_keys, is_secret=False):
        env_string = ''
        try:
            if env_key in os.environ:
                env_string = os.environ[env_key]
            elif is_secret:
                client = boto3.client('secretsmanager')
                response = client.get_secret_value(
                    SecretId=env_key)
                env_string = response.get('SecretString')
            env_json = json.loads(env_string)
            for key in expected_json_keys:
                if key not in env_json:
                    raise Exception(
                        f'Missing key in env var ({env_key}): ({key})')
            return env_json
        except Exception:
            raise Exception(f'Error loading JSON for env var ({env_key})')

    """ Convert Extractor to Context Manager using __enter__ and __exit__"""

    def connect_db(self, user, password, host, port, database):
        if self.engine is None:
            url_object = URL.create(
            "postgresql+pg8000",
            username=user,
            password=password,  # plain (unescaped) text
            host=host,
            port=port,
            database=database
            )
            self.engine = create_engine(url_object)
        if self.conn is None:
            self.conn = self.engine.connect()

    def close(self):
        """ Closes the database connection"""
        if self.conn:
            self.conn.close()
            del self.conn
        if self.engine:
            self.engine.dispose()
            del self.engine

    def read_s3_parquet(self, key):
        try:
            obj = self.s3_client.get_object(Bucket=self.s3_processed_bucket_name,
                                            Key=key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            return df
        except Exception as e:
            raise e

    def write_to_dw(self, table_name, df):
        with self.engine.begin() as con:
            df.to_sql(table_name, con, if_exists='append', index=False)
        
    def load_table(self,key,table_name):
        df = self.read_s3_parquet(key)
        try:
            self.write_to_dw(table_name, df)
        except Exception as e:
            raise e

    def delete_table(self,table):
        with self.engine.begin() as conn:
            conn.execute(text(f'DELETE FROM {table}'))
