import pg8000.native
import src.extraction._env as _env


class Extractor:

    def __init__(self, user=None, password=None, host=None, port=None, database=None):
        user = user or _env.user
        password = password or _env.password
        host = host or _env.host
        port = port or _env.port
        database = database or _env.database
        self.conn = pg8000.native.Connection(
            user, password=password, port=port, database=database, host=host)

    # Convert Extractor to Context Manager using __enter__ and __exit__
    def close(self):
        if self.conn:
            self.conn.close()

    def create_dicts(self, columns, rows):
        return [{column: row[i]
                for (i, column) in enumerate(columns)}
                for row in rows
                ]

    def extract_address(self):
        query_string = "SELECT address_id,address_line_1,address_line_2, district,city,postal_code, country, phone, created_at,last_updated FROM address"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_counter_party(self):
        query_string = "SELECT counterparty_id,counterparty_legal_name,legal_address_id,commercial_contact,delivery_contact,created_at,last_updated FROM Counterparty"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_design(self):
        query_string = "SELECT design_id,created_at,last_updated,design_name,file_location,file_name FROM design"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_sales_order(self,):
        query_string = "SELECT sales_order_id,created_at,last_updated,design_id,staff_id,counterparty_id,units_sold,unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id FROM sales_order"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_transaction(self):
        query_string = "SELECT transaction_id,transaction_type,sales_order_id,purchase_order_id,created_at,last_updated FROM transaction"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_payment_type(self):
        query_string = "SELECT payment_type_id,payment_type_name,created_at,last_updated FROM payment_type"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_payment(self):
        query_string = "SELECT payment_id,created_at,last_updated,transaction_id,counterparty_id,payment_amount,currency_id,payment_type_id,paid,payment_date,company_ac_number,counterparty_ac_number FROM payment"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_currency(self):
        query_string = "SELECT currency_id,currency_code,created_at,last_updated FROM currency"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_staff(self):
        query_string = "SELECT staff_id,first_name,last_name,department_id,email_address,created_at,last_updated FROM staff"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_department(self):
        query_string = "SELECT department_id,department_name,location,manager,created_at,last_updated FROM department"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)

    def extract_purchase_order(self):
        query_string = "SELECT purchase_order_id,created_at,last_updated,staff_id,counterparty_id,item_code,item_quantity,item_unit_price,currency_id,agreed_delivery_date,agreed_payment_date,agreed_delivery_location_id From purchase_order"
        rows = self.conn.run(query_string)
        columns = [meta["name"]for meta in self.conn.columns]
        return self.create_dicts(columns, rows)
