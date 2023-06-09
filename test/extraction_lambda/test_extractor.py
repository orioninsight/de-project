from secret_manager.retrieve_entry import retrieve_entry
from extraction.extractor import Extractor
import pytest
import os
import json


@pytest.fixture(scope='function')
def extractor():
    creds = (os.environ['OI_TOTESYS_DB_INFO']
             if 'OI_TOTESYS_DB_INFO' in os.environ
             else retrieve_entry('OI_TOTESYS_DB_INFO'))
    return Extractor(**json.loads(creds))


def test_connect_to_the_Totesys_database(extractor):
    assert len(extractor.conn.run("SELECT * FROM design")) > 0


def test_extract_address_table(extractor):
    assert set(extractor.extract_address()[0].keys()) == {
        "address_id", "address_line_1", "address_line_2", "district", "city",
        "postal_code", "country", "phone", "created_at", "last_updated"}


def test_extract_counterparty_table(extractor):
    assert set(extractor.extract_counterparty()[0].keys()) == {
        "counterparty_id", "counterparty_legal_name",
        "legal_address_id", "commercial_contact", "delivery_contact",
        "created_at", "last_updated"}


def test_extract_from_design_table(extractor):
    assert set(extractor.extract_design()[0].keys()) == {
        "design_id", "created_at", "last_updated", "design_name",
        "file_location", "file_name"}


def test_extract_from_sales_order_table(extractor):
    assert set(extractor.extract_sales_order()[0].keys()) == {
        "sales_order_id", "created_at", "last_updated", "design_id",
        "staff_id", "counterparty_id", "units_sold", "unit_price",
        "currency_id", "agreed_delivery_date", "agreed_payment_date",
        "agreed_delivery_location_id"}


def test_extract_from_transaction_table(extractor):
    assert set(extractor.extract_transaction()[0].keys()) == {
        "transaction_id", "transaction_type", "sales_order_id",
        "purchase_order_id", "created_at", "last_updated"}


def test_extract_from_payment_type_table(extractor):
    assert set(extractor.extract_payment_type()[0].keys()) == {
        "payment_type_id", "payment_type_name", "created_at", "last_updated"}


def test_extract_from_payment_table(extractor):
    assert set(extractor.extract_payment()[0].keys()) == {
        "payment_id", "created_at", "last_updated", "transaction_id",
        "counterparty_id", "payment_amount", "currency_id",
        "payment_type_id", "paid", "payment_date", "company_ac_number",
        "counterparty_ac_number"}


def test_extract_from_currency_table(extractor):
    assert set(extractor.extract_currency()[0].keys()) == {
        "currency_id", "currency_code", "created_at", "last_updated"}


def test_extract_from_staff_table(extractor):
    assert set(extractor.extract_staff()[0].keys()) == {
        "staff_id", "first_name", "last_name", "department_id",
        "email_address", "created_at", "last_updated"}


def test_extract_from_department_table(extractor):
    assert set(extractor.extract_department()[0].keys()) == {
        "department_id", "department_name", "location", "manager",
        "created_at", "last_updated"}


def test_extract_from_purchase_order_table(extractor):
    assert set(extractor.extract_purchase_order()[0].keys()) == {
        "purchase_order_id", "created_at", "last_updated", "staff_id",
        "counterparty_id", "item_code", "item_quantity", "item_unit_price",
        "currency_id", "agreed_delivery_date", "agreed_payment_date",
        "agreed_delivery_location_id"}


def test_extract_db_stats(extractor):
    assert set(extractor.extract_db_stats().keys()) == {
        "tup_inserted", "tup_updated", "tup_deleted"
        }
