def generate_data():
    count = 0
    for i in range(5):
        order_details_values = ""
        for j in range(10000):
            count += 1
            order_details_values += f"{str(count)},11,14,{(30 + count % 10)},0,{i}\n"
        file = open(f"sample_data/input_{i}.csv", "w")
        file.write(order_details_values)
        file.close()
