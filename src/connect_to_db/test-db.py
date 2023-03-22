import pg8000.native

# Connect to the database with user name postgres
con = pg8000.native.Connection("project_user_3", password="X589fRecpBnpHh", port=5432, database="totesys", host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com")

# Print all the rows in the table
for row in con.run("SELECT * FROM design"):
    print(row)

# # con.close()