from src.connect_to_db.extract_db import (get_db_credentials,extract_db_handler,extract_counter_party,extract_address,extract_design,extract_sales_order,extract_transaction,extract_payment_type,extract_payment,extract_currency,extract_staff,extract_department,extract_purchase_order)
import pytest
import os
import boto3
from moto import mock_secretsmanager
import json
from src.connect_to_db.connection import conn

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture(scope="function")
def sm(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")

@pytest.fixture(scope="function")
def sm_secret(sm):
    db_creds = json.dumps({"user": "abcd", "password":"1234","port":"1000","database":"books", "host" : "localhost"})
    sm.create_secret(Name="db007", SecretString= db_creds)
    return sm

def test_connect_to_the_Totesys_database():
    assert len(conn.run("SELECT * FROM design")) > 0

def test_returns_db_credentials_from_secretsmanager(sm_secret):
    # print(dir(sm))
    
    db_creds = get_db_credentials("db007")
    assert db_creds["user"] == "abcd"
    assert db_creds["password"] == "1234"
    assert db_creds["database"] == "books"

def test_extract_address_table():
    assert set(extract_address()[0].keys()) == {"address_id","address_line_1","address_line_2", "district","city","postal_code", "country", "phone", "created_at","last_updated"}

def test_extract_counter_party_table():
    assert set(extract_counter_party()[0].keys()) =={"counterparty_id","counterparty_legal_name","legal_address_id","commercial_contact","delivery_contact","created_at","last_updated"}
    
def test_extract_from_design_table():
    assert set(extract_design()[0].keys()) == {"design_id","created_at","last_updated","design_name","file_location","file_name"}

def test_extract_from_sales_order_table():
    assert set(extract_sales_order()[0].keys()) == {"sales_order_id","created_at","last_updated","design_id","staff_id","counterparty_id","units_sold","unit_price","currency_id","agreed_delivery_date","agreed_payment_date","agreed_delivery_location_id"}
    
def test_extract_from_transaction_table():
    assert set(extract_transaction()[0].keys()) == {"transaction_id","transaction_type","sales_order_id","purchase_order_id","created_at","last_updated"}
    
def test_extract_from_payment_type_table():
    assert set(extract_payment_type()[0].keys()) == {"payment_type_id","payment_type_name","created_at","last_updated"}
    
def test_extract_from_payment_table():
    assert set(extract_payment()[0].keys()) == {"payment_id","created_at","last_updated","transaction_id","counterparty_id","payment_amount","currency_id","payment_type_id","paid","payment_date","company_ac_number","counterparty_ac_number"}
    
def test_extract_from_currency_table():
    assert set(extract_currency()[0].keys()) == {"currency_id","currency_code","created_at","last_updated"}
    
def test_extract_from_staff_table():
    assert set(extract_staff()[0].keys()) =={"staff_id","first_name","last_name","department_id","email_address","created_at","last_updated"}
    
def test_extract_from_department_table():
    assert set(extract_department()[0].keys()) == {"department_id","department_name","location","manager","created_at","last_updated"}
    
def test_extract_from_purchase_order_table():
    assert set(extract_purchase_order()[0].keys()) == {"purchase_order_id","created_at","last_updated","staff_id","counterparty_id","item_code","item_quantity","item_unit_price","currency_id","agreed_delivery_date","agreed_payment_date","agreed_delivery_location_id"}