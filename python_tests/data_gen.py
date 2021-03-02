def generate_data():
    count = 1
    for i in range(5):
        order_details_values = ""
        order_values = ""
        for j in range(10000):
            count += 1
            order_details_values += str(count // 2) + "," + str(1 + count % 70) + "," + str(1 + count % 53) + "," \
                                    + str(30 + count % 10) + ",0," + str(i) + "\n"
            if count % 2 == 0:
                order_values += str(count // 2) + ", '1', 1, '2020-01-01', '2020-01-10', '2020-01-03', 1, 3.0, " \
                                                 "'ship name', 'ship address', 'city', 'region', '90210', '" \
                                                  "Country" + str(count // 2 % 71) + "'\n"
        file = open("sample_data/input_" + str(i) + ".csv", "w")
        file.write(order_details_values)
        file.close()
        orders_file = open("sample_data/orders_" + str(i) + ".csv", "w")
        orders_file.write(order_values)
        orders_file.close()

# orders_schema = StructType() \
#     .add("order_id", "integer") \
#     .add("customer_id", "string") \
#     .add("employee_id", "integer") \
#     .add("order_date", "date") \
#     .add("required_date", "date") \
#     .add("shipped_date", "date") \
#     .add("ship_via", "integer") \
#     .add("freight", "float") \
#     .add("ship_name", "string") \
#     .add("ship_address", "string") \
#     .add("ship_city", "string") \
#     .add("ship_region", "string") \
#     .add("ship_postal_code", "string") \
#     .add("ship_country", "string")
