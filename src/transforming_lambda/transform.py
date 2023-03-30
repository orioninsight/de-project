import pandas as pd
import boto3
import logging


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def transform_lambda_handler(event, context):
    transformer = Transformer()
    files = transformer.list_csv_files()
    for file in files:
        transform_fn = getattr(transformer, f'transform_{file}')
        transformer.store_parquet(transform_fn(transformer.read_csv(file)))
    transformer.store_parquet(transformer.create_dim_date())


class Transformer:

    FILE_LIST = ['address', 'design', 'counterparty',
                 'purchase_order', 'staff', 'sales_order',
                 'payment', 'transaction', 'payment_type',
                 'currency', 'department']

    def __init__(self, bucket_name):
        self.s3_client = boto3.client('s3')
        self.s3_bucket_name = bucket_name

    def list_csv_files(self):
        ingestion_csv_files = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket_name)['Contents']
        for file in Transformer.FILE_LIST:
            if file not in [item['Key'] for item in ingestion_csv_files]:
                raise Exception('Files are not complete')
        return Transformer.FILE_LIST

    def read_csv(self, key):
        try:
            obj = self.s3_client.get_object(Bucket=self.s3_bucket_name,
                                            Key=key)
            df = pd.read_csv(obj['Body'])
            return df
        except Exception as e:
            logger.error(f'An error occurred reading csv file: {e}')
            raise Exception()

    def transform_currency(self, df_currency):
        return df_currency.drop(columns=['created_at', 'last_updated'])

    def transform_design(self, df_design):
        return df_design.drop(columns=['created_at', 'last_updated'])

    def transform_address(self, df_address):
        return df_address.drop(columns=['created_at', 'last_updated']).rename(
            columns={'address_id': 'location_id'})

    def create_dim_date(self, from_date_string='2022-11-3',
                        to_date_string='2023-5-1'):
        df = pd.DataFrame(pd.date_range(
            from_date_string, to_date_string), columns=['date'])
        df['date_id'] = df['date'].dt.strftime('%Y%m%d').astype(int)
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek
        df['day_name'] = df['date'].dt.strftime("%A")
        df['month_name'] = df['date'].dt.strftime("%B")
        df['quarter'] = df['date'].dt.quarter
        return df.loc[:, df.columns != 'date']

    def store_parquet():
        pass
