
import pg8000.native
import logging
import boto3
from botocore.exceptions import ClientError
import json
from src.connection import conn

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def create_dicts(columns, rows):
    return [{column: row[i]
            for (i, column) in enumerate(columns)}
        for row in rows
        ]

def extract_address():
    query_string = "SELECT address_id,address_line_1,address_line_2, district,city,postal_code, country, phone, created_at,last_updated FROM address"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_counter_party():
    query_string = "SELECT counterparty_id,counterparty_legal_name,legal_address_id,commercial_contact,delivery_contact,created_at,last_updated FROM Counterparty"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_design():
    query_string = "SELECT design_id,created_at,last_updated,design_name,file_location,file_name FROM design"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)
    
def extract_sales_order():
    query_string = "SELECT sales_order_id,created_at,last_updated,design_id,staff_id,counterparty_id,units_sold,unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id FROM sales_order"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_transaction():
    query_string = "SELECT transaction_id,transaction_type,sales_order_id,purchase_order_id,created_at,last_updated FROM transaction"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_payment_type():
    query_string = "SELECT payment_type_id,payment_type_name,created_at,last_updated FROM payment_type"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_payment():
    query_string = "SELECT payment_id,created_at,last_updated,transaction_id,counterparty_id,payment_amount,currency_id,payment_type_id,paid,payment_date,company_ac_number,counterparty_ac_number FROM payment"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_currency():
    query_string = "SELECT currency_id,currency_code,created_at,last_updated FROM currency"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

def extract_staff():
    query_string = "SELECT staff_id,first_name,last_name,department_id,email_address,created_at,last_updated FROM staff"
    rows = conn.run(query_string)
    columns = [meta["name"]for meta in conn.columns]
    return create_dicts(columns,rows)

    
    
def extract_db_handler(event, context):
    # create connection and get credentials
    # extract tables and store as files
    pass


def get_db_credentials(secret_id):
    sm_secret = boto3.client("secretsmanager")
    secret_string = sm_secret.get_secret_value(SecretId = secret_id)["SecretString"]
    return json.loads(secret_string)