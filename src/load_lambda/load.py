import pandas as pd
import boto3
import logging
import os
import json
from io import BytesIO
from sqlalchemy import URL
from sqlalchemy import create_engine, text


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def loader_handler(event, context):
    dw_secret_json = load_env_var('OI_TOTESYS_DW_INFO',
                                  ['host', 'port', 'user',
                                   'password', 'database'], True)
    s3_processed_bucket_name = load_env_var(
        'OI_PROCESSED_INFO', ['s3_bucket_name'])['s3_bucket_name']
    loader = Loader(s3_processed_bucket_name)
    loader.connect_db(**dw_secret_json)
    # Delete in reverse order, starting with fact tables, to comply
    # With integrity constraints
    for table in list(Loader.FILE_LIST.values())[::-1]:
        loader.delete_table(table)
    # Load all dim tables, then fact table (ordered last)
    for key, table_name in loader.FILE_LIST.items():
        loader.load_table(key, table_name)


def load_env_var(env_key, expected_json_keys, is_secret=False):
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


class Loader:

    FILE_LIST = {'address': 'dim_location',
                 'design': 'dim_design',
                 'counterparty': 'dim_counterparty',
                 'staff': 'dim_staff',
                 'currency': 'dim_currency',
                 'date': 'dim_date',
                 'payment_type': 'dim_payment_type',
                 'transaction': 'dim_transaction',
                 'purchase_order': 'fact_purchase_order',
                 'payment': 'fact_payment',
                 'sales_order': 'fact_sales_order'}

    conn = None
    engine = None

    def __init__(self, bucket_name):
        self.s3_client = boto3.client('s3')
        self.s3_processed_bucket_name = bucket_name

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
            obj = self.s3_client.get_object(
                Bucket=self.s3_processed_bucket_name, Key=key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            return df
        except Exception as e:
            raise e

    def write_to_dw(self, table_name, df):
        with self.engine.begin() as con:
            df.to_sql(table_name, con, if_exists='append', index=False)

    def load_table(self, key, table_name):
        df = self.read_s3_parquet(key)
        if key == 'date':
            df['date_id'] = pd.to_datetime(
                df['date_id'], format='%Y-%m-%d').dt.date
        elif key == 'sales_order':
            df['created_date'] = pd.to_datetime(
                df['created_date'], format='%Y-%m-%d').dt.date
            df['created_time'] = pd.to_datetime(
                df['created_time'], infer_datetime_format=True).dt.time
            df['last_updated_time'] = pd.to_datetime(
                df['last_updated_time'], infer_datetime_format=True).dt.time
            df['last_updated_date'] = pd.to_datetime(
                df['last_updated_date'], format='%Y-%m-%d').dt.date
            df['agreed_delivery_date'] = pd.to_datetime(
                df['agreed_delivery_date'], format='%Y-%m-%d').dt.date
            df['agreed_payment_date'] = pd.to_datetime(
                df['agreed_payment_date'], format='%Y-%m-%d').dt.date
        elif key == 'purchase_order':
            df['created_date'] = pd.to_datetime(
                df['created_date'], format='%Y-%m-%d').dt.date
            df['created_time'] = pd.to_datetime(
                df['created_time'], infer_datetime_format=True).dt.time
            df['last_updated_date'] = pd.to_datetime(
                df['last_updated_date'], format='%Y-%m-%d').dt.date
            df['last_updated_time'] = pd.to_datetime(
                df['last_updated_time'], infer_datetime_format=True).dt.time
            df['agreed_delivery_date'] = pd.to_datetime(
                df['agreed_delivery_date'], format='%Y-%m-%d').dt.date
            df['agreed_payment_date'] = pd.to_datetime(
                df['agreed_payment_date'], format='%Y-%m-%d').dt.date
        elif key == 'payment':
            df['created_date'] = pd.to_datetime(
                df['created_date'], format='%Y-%m-%d').dt.date
            df['created_time'] = pd.to_datetime(
                df['created_time'], infer_datetime_format=True).dt.time
            df['last_updated_date'] = pd.to_datetime(
                df['last_updated_date'], format='%Y-%m-%d').dt.date
            df['last_updated_time'] = pd.to_datetime(
                df['last_updated_time'], infer_datetime_format=True).dt.time
            df.rename(columns={'last_updated_time': 'last_updated'},
                      inplace=True)
            df['payment_date'] = pd.to_datetime(
                df['payment_date'], format='%Y-%m-%d').dt.date
        try:
            self.write_to_dw(table_name, df)
        except Exception as e:
            raise e

    def delete_table(self, table):
        with self.engine.begin() as conn:
            conn.execute(text(f'DELETE FROM {table}'))
