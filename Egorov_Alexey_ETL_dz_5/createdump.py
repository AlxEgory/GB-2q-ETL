import psycopg2

conn_string= "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    customer = "COPY customer TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('customer.csv', 'w') as f:
        cursor.copy_expert(customer, f)
    lineitem = "COPY lineitem TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('lineitem.csv', 'w') as f:
        cursor.copy_expert(lineitem, f)
    orders = "COPY orders TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('orders.csv', 'w') as f:
        cursor.copy_expert(orders, f)
    partsupp = "COPY partsupp TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('partsupp.csv', 'w') as f:
        cursor.copy_expert(partsupp, f)
    supplier = "COPY supplier TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('supplier.csv', 'w') as f:
        cursor.copy_expert(supplier, f)
    part = "COPY part TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('part.csv', 'w') as f:
        cursor.copy_expert(part, f)
    region = "COPY region TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('region.csv', 'w') as f:
        cursor.copy_expert(region, f)
    nation = "COPY nation TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('nation.csv', 'w') as f:
        cursor.copy_expert(nation, f)
