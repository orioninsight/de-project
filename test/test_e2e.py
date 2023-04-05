import json
from pathlib import Path
import pytest
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


@pytest.fixture(scope="module")
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
                         'agreed_delivery_location_id'}),
        ('purchase_order', {'purchase_record_id', 'purchase_order_id',
                            'created_date',
                            'created_time', 'last_updated_date',
                            'last_updated_time',
                            'staff_id', 'item_code',
                            'counterparty_id', 'item_quantity',
                            'item_unit_price',
                            'currency_id', 'agreed_payment_date',
                            'agreed_delivery_date',
                            'agreed_delivery_location_id'}),
        ('payment', {'payment_record_id', 'payment_id', 'created_date',
                     'created_time', 'last_updated_date',
                     'last_updated_time', 'transaction_id',
                     'payment_amount', 'counterparty_id', 'payment_type_id',
                     'paid', 'currency_id', 'payment_date'}),
        ('payment_type', {'payment_type_id', 'payment_type_name'}),
        ('transaction', {'transaction_id', 'transaction_type',
                         'sales_order_id', 'purchase_order_id'})
    ], ids=lambda x: x[0])
def parquet_file(request):
    key, cols = request.param
    file_name = f'/tmp/{key}'
    yield (key, file_name, cols)
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
    os.environ['OI_LOAD_LAMBDA_INFO'] = json.dumps(
        {'load_lambda_arn': 'AN ARN'})


@patch('src.extraction_lambda.extract_db.call_transform_lambda',
       return_value=None)
@patch('src.transform_lambda.transform.call_loader_lambda',
       return_value=None)
def test_data_flows_from_extraction_to_transform(
        mock_tf_lambda, mock_ld_lambda, s3, env_vars, parquet_file):
    key, file, expected_df_cols = parquet_file

    extract_db_handler({}, None)
    transform_handler({}, None)

    s3.download_file(
            PROCESSED_BUCKET_NAME, key, f'{file}.parq')
    pf = fp.ParquetFile(f'{file}.parq')
    res_df = pf.to_pandas()

    assert set(res_df.columns) == expected_df_cols