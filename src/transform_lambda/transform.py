import pandas as pd
import boto3
import logging
from fastparquet import write


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

    def __init__(self, bucket_name, processed_bucket_name):
        self.s3_client = boto3.client('s3')
        self.s3_bucket_name = bucket_name
        self.s3_processed_bucket_name = processed_bucket_name

    def list_csv_files(self):
        ingestion_csv_files = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket_name)['Contents']
        for file in Transformer.FILE_LIST:
            if file not in [item['Key'] for item in ingestion_csv_files]:
                msg = 'ERROR: Files are not complete'
                logger.error(msg)
                raise Exception(msg)

        return Transformer.FILE_LIST

    def read_csv(self, key):
        try:
            obj = self.s3_client.get_object(Bucket=self.s3_bucket_name,
                                            Key=key)
            df = pd.read_csv(obj['Body'])
            return df
        except Exception as e:
            logger.error(f'An error occurred reading csv file: {e}')
            raise RuntimeError()

    def store_as_parquet(self, file_name, df):
        if not isinstance(df, pd.DataFrame):
            msg = 'ERROR: object not a dataframe'
            logger.error(msg)
            raise ValueError(msg)

        if not type(file_name) == str:
            msg = 'ERROR: file_name expects a string'
            logger.error(msg)
            raise TypeError(msg)

        try:
            write(f'/tmp/{file_name}.parq', df)

        except Exception as e:
            msg = f'An error occurred converting dataframe to parquet: {e}'
            logger.error(msg)
            raise Exception(msg)

        try:
            self.s3_client.upload_file(
                f'/tmp/{file_name}.parq', self.s3_processed_bucket_name,
                file_name)

        except Exception as e:
            msg = f'An error occurred writing parquet file to bucket: {e}'
            logger.error(msg)
            raise Exception(msg)

    def transform_currency(self, df_currency):
        try:
            df_currency_info = pd.read_csv('src/transform_lambda/currency.csv')
        except Exception as e:
            logger.error(f'Could not read currency.csv: {e}')
            raise RuntimeError()
        df_currency = df_currency.join(
            df_currency_info.set_index('currency_code'),
            on='currency_code', how='left')
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

    def transform_staff(self, df_staff, df_department):
        staff_table = df_staff.drop(
            columns=['created_at', 'last_updated'])
        department_table = df_department.drop(
            columns=['created_at', 'last_updated', 'manager'])
        merged_table = pd.merge(
            staff_table, department_table, on='department_id')
        return merged_table.drop(columns=['department_id'])

    def store_parquet():
        pass
