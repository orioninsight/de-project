import json
import datetime
from unittest.mock import patch
from extraction.monitor import Monitor
import pytest
import os
from moto import mock_s3
import boto3
from extraction.extractor import Extractor
from secret_manager.retrieve_entry import retrieve_entry

# monitor returns boolean

S3_TEST_BUCKET_NAME = "test-bucket"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=S3_TEST_BUCKET_NAME)
        yield s3_client


@pytest.fixture(scope='function')
@patch.object(Extractor, 'create_connection')
def extractor(mock_conn):
    creds = (os.environ['OI_TOTESYS_SECRET_STRING']
             if 'OI_TOTESYS_SECRET_STRING' in os.environ
             else retrieve_entry('totesys_db'))
    return Extractor(**json.loads(creds))


@pytest.fixture(scope="function")
def monitor(extractor):
    return Monitor(S3_TEST_BUCKET_NAME, extractor)


@patch('extraction.extractor.Extractor.extract_db_stats',
       return_value={"tup_deleted": 0, "tup_updated": 0, "tup_inserted": 0})
def test_has_state_changed_is_false_given_unchanged_state(m, monitor):
    monitor.current_state = {"tup_deleted": 0, "tup_updated": 0, "tup_inserted": 0}
    assert not monitor.has_state_changed()


@patch('extraction.extractor.Extractor.extract_db_stats',
       return_value={"tup_deleted": 2, "tup_updated": 1, "tup_inserted": 0})
def test_has_state_changed_returns_true_given_changed_state(m, monitor):
    assert monitor.has_state_changed()


@patch('extraction.extractor.Extractor.extract_db_stats',
       return_value={"tup_deleted": 2, "tup_updated": 1, "tup_inserted": 0})
def test_get_db_stats_updates_current_state(m, monitor):
    monitor.get_db_stats()
    assert set(monitor.new_state.keys()) == {
        "tup_inserted", "tup_updated", "tup_deleted"}


def test_get_current_state_returns_1_if_key_exists(s3, monitor):
    db_state = {"tup_deleted": 0, "tup_updated": 0, "tup_inserted": 0}
    s3.put_object(Bucket=S3_TEST_BUCKET_NAME,
                  Body=json.dumps(db_state), Key=Monitor.DB_STATE_KEY)
    assert monitor.get_current_state() == 1
   

def test_get_current_state_returns_minus_1_if_s3_key_does_not_exist(s3,
                                                                    monitor):
    assert monitor.get_current_state() == -1


def test_get_current_state_returns_0_if_stats_json_missing_key(s3, monitor):
    db_state = {"tup_delete": 0, "tup_update": 0, "tup_inserted": 0}
    s3.put_object(Bucket=S3_TEST_BUCKET_NAME,
                  Body=json.dumps(db_state), Key=Monitor.DB_STATE_KEY)
    assert monitor.get_current_state() == 0


def test_get_current_state_returns_0_if_stats_json_non_int_values(s3, monitor):
    db_state = {"tup_deleted": 0, "tup_updated": '0', "tup_inserted": 0}
    s3.put_object(Bucket=S3_TEST_BUCKET_NAME,
                  Body=json.dumps(db_state), Key=Monitor.DB_STATE_KEY)
    assert monitor.get_current_state() == 0


def test_get_current_state_returns_0_if_stats_json_missing_key(s3, monitor):
    db_state = {"tup_deleted": 0, "tup_updated": 0}
    s3.put_object(Bucket=S3_TEST_BUCKET_NAME,
                  Body=json.dumps(db_state), Key=Monitor.DB_STATE_KEY)
    assert monitor.get_current_state() == 0
    

def test_save_state_saves_new_state_to_s3_bucket(s3,monitor):
    with patch('extraction.monitor.datetime') as mock_date:
        mock_date.now.return_value = datetime.datetime(2023,3,27,1,2,3,4)
        monitor.new_state = {"tup_deleted": 2, "tup_updated": 1, "tup_inserted": 2}
        monitor.save_state()
        
        obj = s3.get_object(Bucket=S3_TEST_BUCKET_NAME, Key=Monitor.DB_STATE_KEY)
        test_stats = json.loads(obj['Body'].read())
        
        assert test_stats == monitor.new_state
        assert test_stats ['retrieved_at'] == 1679875323.000004
    
    
    