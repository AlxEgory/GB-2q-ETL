import psycopg2

conn_string= "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    customer = "COPY customer from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('customer.csv', 'r') as f:
        cursor.copy_expert(customer, f)
    lineitem = "COPY lineitem from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('lineitem.csv', 'r') as f:
        cursor.copy_expert(lineitem, f)
    orders = "COPY orders from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('orders.csv', 'r') as f:
        cursor.copy_expert(orders, f)
    partsupp = "COPY partsupp from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('partsupp.csv', 'r') as f:
        cursor.copy_expert(partsupp, f)
    supplier = "COPY supplier from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('supplier.csv', 'r') as f:
        cursor.copy_expert(supplier, f)
    part = "COPY part from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('part.csv', 'r') as f:
        cursor.copy_expert(part, f)
    region = "COPY region from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('region.csv', 'r') as f:
        cursor.copy_expert(region, f)
    nation = "COPY nation from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('nation.csv', 'r') as f:
        cursor.copy_expert(nation, f)