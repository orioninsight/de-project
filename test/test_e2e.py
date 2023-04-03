import json
from pathlib import Path
import pytest
from moto import mock_s3
import os
import boto3
from unittest.mock import patch
from src.extraction_lambda.extract_db import extract_db_handler
from src.transform_lambda.transform import transform_handler
import fastparquet as fp
from datetime import datetime

INGESTION_BUCKET_NAME = f'''test-ingestion-bucket-{
   int(datetime.now().timestamp())}'''
PROCESSED_BUCKET_NAME = f'''test-processed-bucket-{
   int(datetime.now().timestamp())}'''


# @pytest.fixture(scope="function")
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
#                 'AWS_SECURITY_TOKEN', 'AWS_SESSION_TOKEN',
#                 'AWS_DEFAULT_REGION']
#     old_env_vars = {var: os.environ.get(var, None) for var in env_vars}
#     #print(old_env_vars)

#     os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#     os.environ["AWS_SECURITY_TOKEN"] = "testing"
#     os.environ["AWS_SESSION_TOKEN"] = "testing"
#     os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
#     yield
#     for var in env_vars:
#         if old_env_vars[var] is not None:
#             os.environ[var] = old_env_vars[var]
#         else:
#             del os.environ[var]


@pytest.fixture(scope="function")
def s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket=INGESTION_BUCKET_NAME)
    s3_client.create_bucket(Bucket=PROCESSED_BUCKET_NAME)
    yield s3_client
    for bucket_name in [INGESTION_BUCKET_NAME, PROCESSED_BUCKET_NAME]:
        objs = s3_client.list_objects_v2(Bucket=bucket_name)['Contents']
        for obj in objs:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        s3_client.delete_bucket(Bucket=bucket_name)

# @pytest.fixture(scope="function")
# def s3():
#     s3_client = boto3.client("s3")
#     bucket_list = [INGESTION_BUCKET_NAME, PROCESSED_BUCKET_NAME]
#     for bucket in bucket_list:
#         s3_client.create_bucket(Bucket=bucket)
#     yield s3_client
#     for bucket in bucket_list:
#         objs = s3_client.list_objects_v2(Bucket=bucket)['Contents']
#         for obj in objs:
#             s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
#         s3_client.delete_bucket(Bucket=bucket)


@pytest.fixture(
    scope='function', params=[
        ('address', {'location_id', 'address_line_1', 'address_line_2',
                     'district', 'city', 'postal_code', 'country', 'phone'}),
        ('currency', {'currency_code', 'currency_id', 'currency_name'}),
        ('design', {'design_id', 'design_name',
                    'file_location', 'file_name'}),
        ('staff', {'staff_id', 'first_name', 'last_name',
                   'department_name', 'location', 'email_address'}),
        ('counterparty', {'counterparty_id',
                          'counterparty_legal_name',
                          'counterparty_legal_address_line_1',
                          'counterparty_legal_address_line_2',
                          'counterparty_legal_district',
                          'counterparty_legal_city',
                          'counterparty_legal_postal_code',
                          'counterparty_legal_country',
                          'counterparty_legal_phone_number'}),
        ('date', {'date_id', 'year', 'month', 'day', 'day_of_week',
                  'day_name', 'month_name', 'quarter'}),
        ('sales_order', {'sales_record_id', 'created_date',
                         'created_time', 'last_updated_date',
                         'last_updated_time',
                         'sales_order_id', 'sales_staff_id',
                         'counterparty_id', 'units_sold', 'unit_price',
                         'currency_id', 'design_id', 'agreed_payment_date',
                         'agreed_delivery_date',
                         'agreed_delivery_location_id'})
    ], ids=lambda x: x[0])
def parquet_file(request):
    file_name, cols = request.param
    yield request.param
    file = Path(f'{file_name}.parq')
    if file.is_file():
        file.unlink()


@pytest.fixture(scope='function')
def env_vars():
    os.environ['OI_STORER_INFO'] = json.dumps(
        {'s3_bucket_name': INGESTION_BUCKET_NAME})
    os.environ['OI_TRANSFORM_LAMBDA_INFO'] = json.dumps(
        {'transform_lambda_arn': 'AN ARN'})
    os.environ['OI_PROCESSED_INFO'] = json.dumps(
        {'s3_bucket_name': PROCESSED_BUCKET_NAME})


@patch('src.extraction_lambda.extract_db.call_transform_lambda',
       return_value=None)
def test_currency_data_flows_from_extraction_to_transform(
        mock_tf_lambda, s3, env_vars, parquet_file):
    file, expected_df_cols = parquet_file

    extract_db_handler({}, None)
    transform_handler({}, None)

    s3.download_file(
            PROCESSED_BUCKET_NAME, file, f'{file}.parq')
    pf = fp.ParquetFile(f'{file}.parq')
    res_df = pf.to_pandas()

    assert set(res_df.columns) == expected_df_cols
