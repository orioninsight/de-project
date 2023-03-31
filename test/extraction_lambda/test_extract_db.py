import json
from pathlib import Path
from unittest.mock import patch
from extract_db import (extract_db_handler, extract_db_helper, load_env_var,
                        VALID_TABLES)
from extraction.extractor import Extractor
from extraction.monitor import Monitor
import pytest
import boto3
import os
from datetime import datetime


S3_TEST_BUCKET_NAME = f'''test-extraction-bucket-{
   int(datetime.now().timestamp())}'''


@pytest.fixture(scope='function')
def info():
    os.environ['OI_STORER_INFO'] = f'''{{"s3_bucket_name":
                                        "{S3_TEST_BUCKET_NAME}"}}'''
    os.environ['OI_TRANSFORM_LAMBDA_INFO'] = '{"transform_lambda_arn":"ARN"}'


@pytest.fixture(scope="function")
def s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=S3_TEST_BUCKET_NAME)
    yield s3_client
    objs = s3_client.list_objects_v2(Bucket=S3_TEST_BUCKET_NAME)['Contents']
    for obj in objs:
        s3_client.delete_object(Bucket=S3_TEST_BUCKET_NAME, Key=obj['Key'])
    s3_client.delete_bucket(Bucket=S3_TEST_BUCKET_NAME)


@pytest.fixture(scope='function')
def extractor():
    return Extractor()


@pytest.fixture(scope='function', params=['address', 'design', 'counterparty',
                                          'purchase_order', 'staff',
                                          'sales_order', 'payment',
                                          'transaction', 'payment_type',
                                          'currency', 'department'])
def downloaded_file(request):
    file_name = f"/tmp/{request.param}.csv"
    yield request.param, file_name
    file = Path(file_name)
    if file.is_file():
        file.unlink()


@pytest.fixture(scope='function', params=['OI_STORER_INFO',
                                          'OI_TRANSFORM_LAMBDA_INFO'])
def unset_set_env(request):
    db_secret_string = os.environ.get(request.param, None)
    if db_secret_string is not None:
        del os.environ[request.param]
    yield request.param
    if db_secret_string is not None:
        os.environ[request.param] = db_secret_string


def test_raises_unsupported_table_exception(info):
    with pytest.raises(Exception, match='Unsupported table'):
        extract_db_helper(['UNSUPPORTED_TABLE'])


def test_raises_exception_given_lambda_payload(info):
    with pytest.raises(Exception, match='payload requires list in'):
        extract_db_handler({'extract_table': 123}, None)


# @pytest.mark.skip()
@patch('extract_db.call_transformation_lambda')
def test_extracts_db_table_and_stores_file_in_s3(mock_tf_lambda, s3,
                                                 info,
                                                 downloaded_file):
    table_name, file_name = downloaded_file
    extract_db_handler({'extract_table': [table_name]}, None)
    s3.download_file(S3_TEST_BUCKET_NAME,
                     table_name, file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        assert f'{table_name}_id' in f.readline().split(',')
        assert len(f.readlines()) > 0


@patch('extract_db.call_transformation_lambda')
@patch('extract_db.extract_db_helper')
@patch('extract_db.Monitor.has_state_changed', return_value=True)
def test_extracts_all_db_tables_given_no_payload(
        mock_monitor, mock_db_helper, mock_tf_lambda, info):
    extract_db_handler({}, None)
    mock_db_helper.assert_called_once_with(VALID_TABLES)


@patch('extract_db.call_transformation_lambda')
@patch('extract_db.extract_db_helper')
def test_extraction_runs_if_no_state_file_and_else_not(
        mock_db_helper, mock_tf_lambda, s3):
    extract_db_handler({}, None)
    obj = s3.get_object(Bucket=S3_TEST_BUCKET_NAME, Key=Monitor.DB_STATE_KEY)
    test_stats = json.loads(obj['Body'].read())
    for tup_key in test_stats:
        assert test_stats[tup_key] >= 0
    # Re-run extraction lambda
    extract_db_handler({}, None)
    obj2 = s3.get_object(Bucket=S3_TEST_BUCKET_NAME, Key=Monitor.DB_STATE_KEY)
    test_stats2 = json.loads(obj2['Body'].read())
    for tup_key in Monitor.STAT_KEYS:
        assert test_stats2[tup_key] == test_stats[tup_key]
    assert test_stats2['retrieved_at'] >= test_stats['retrieved_at']
    mock_db_helper.assert_called_once_with(VALID_TABLES)


@patch('extract_db.extract_db_helper')
@patch('extraction.monitor.Monitor.has_state_changed', return_value=True)
@patch('extract_db.call_transformation_lambda')
def test_extraction_calls_transformation_lambda_if_db_changed(
        mock_call_tf_lambda, mock_monitor, mock_db_helper, info):
    extract_db_handler({}, None)
    mock_call_tf_lambda.assert_called_once()


@patch('extract_db.retrieve_entry',
       return_value='{"host": "", "port": "", "user": "",'
       '"password": "", "database": ""}')
def test_extraction_raises_error_if_missing_env_var(mock_retrieve,
                                                    unset_set_env):
    with pytest.raises(Exception, match=unset_set_env):
        extract_db_handler({}, None)


@patch('extract_db.retrieve_entry', return_value=None)
def test_extraction_raises_error_if_missing_db_env_var(mock_retrieve):
    env_var = 'OI_TOTESYS_DB_INFO'
    db_secret_string = os.environ.get(env_var, None)

    if db_secret_string is not None:
        del os.environ[env_var]

    with pytest.raises(Exception, match=env_var):
        extract_db_handler({}, None)

    if db_secret_string is not None:
        os.environ[env_var] = db_secret_string


@patch('extract_db.retrieve_entry',
       return_value='{"HELLO": "ORION", "WORLD": "INSIGHTS"}')
def test_load_vars_uses_secret_manager_if_is_secret_set_to_true(mock_retrieve):
    no_such_env_key = f'''_TEST_{int(datetime.now().timestamp())}'''
    expected_keys = ['HELLO', 'WORLD']

    res_json = load_env_var(no_such_env_key, expected_keys, True)

    mock_retrieve.assert_called_once_with(no_such_env_key)
    assert res_json == {"HELLO": "ORION", "WORLD": "INSIGHTS"}


def test_load_vars_raises_error_if_env_var_contains_invalid_keys():
    env_key = f'''_TEST_{int(datetime.now().timestamp())}'''
    expected_keys = ['HELLO', 'WORLD']
    os.environ[env_key] = '{"HELL":"ORION", "WOLD":"INSIGHTS"}'
    with pytest.raises(Exception, match='Error loading JSON for env var'):
        load_env_var(env_key, expected_keys)
