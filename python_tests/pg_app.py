import time

import psycopg2


def print_all(rows):
    for row in rows:
        print(row[0])


def get_max_id(cursor):
    cursor.execute("SELECT max(order_id) FROM orders;")
    return cursor.fetchone()[0]


def postgres_requests():
    connection = psycopg2.connect(dbname='northwind', user='postgres', host='localhost', password='')
    count_cursor = connection.cursor()
    start_id = get_max_id(count_cursor)
    count = start_id

    for i in range(5):
        print(f"\nIteration {i}")
        order_values = ""
        order_details_values = ""
        for j in range(100 * i):
            count += 1
            order_values += "(" + str(count) + ",'ERNSH',1,'1996-07-04', '1996-08-01', '1996-07-16',1,140.51," \
                                               "'ErnstHandel','Kirchgasse6','Graz',0,8010,'Austria'),"
            order_details_values += f"({str(count)}, 11, 14, {(45 + count % 10)}, 0),"
        order_values = order_values[:-1]
        order_details_values = order_details_values[:-1]
        cursor = connection.cursor()
        cursor.execute("EXPLAIN ANALYZE INSERT INTO orders VALUES" + order_values)
        print_all(cursor)
        cursor.execute("EXPLAIN ANALYZE INSERT INTO order_details VALUES" + order_details_values)
        print_all(cursor)
        cursor.execute("EXPLAIN ANALYZE DELETE FROM order_details WHERE "
                       "ctid IN (SELECT ctid FROM orders ORDER BY order_id LIMIT 100)")
        # simple selection
        # cursor.execute("EXPLAIN ANALYZE SELECT * FROM bulk_orders;")
        # simple aggregate
        cursor.execute("EXPLAIN ANALYZE SELECT * FROM ordered_item_count;")
        print_all(cursor)
        time.sleep(1)
        cursor.close()

    last_id = get_max_id(count_cursor)
    print(f"\nLast inserted ID: {last_id}\n")
    connection.commit()
    count_cursor.close()
    connection.close()
    # print(cursor)
    # rows = cursor.fetchall()
    # counter = 0
    # for row in rows:
    #     counter += 1
    #     print(row[0])
