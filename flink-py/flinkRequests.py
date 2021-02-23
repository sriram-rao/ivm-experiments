from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import *


def flink_request():
    # environment configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    table_env = StreamTableEnvironment.create(env)

    # register Orders table and Result table sink in table environment
    source_data_path = "./input/"
    result_data_path = "./output/"
    source_ddl = f"""
            create table order_details(
                order_id INT,
                product_id INT,
                unit_price FLOAT,
                quantity INT,
                discount FLOAT,
                input_number INT,
                rowtime AS PROCTIME()
            ) with (
                'connector' = 'filesystem',
                'format' = 'csv',
                'path' = '{source_data_path}'
            )
            """
    table_env.execute_sql(source_ddl)

    sink_ddl = f"""
        create table `Result`(
                order_id INT,
                product_id INT,
                unit_price FLOAT,
                quantity INT,
                discount FLOAT,
                input_number INT,
                rowtime TIMESTAMP(3)
        ) with (
            'connector' = 'filesystem',
            'format' = 'csv',
            'path' = '{result_data_path}'
        )
        """
    table_env.execute_sql(sink_ddl)

    # specify table program
    # tweets = table_env.from_path("order_details")  # schema (a, b, c, rowtime)
    result = table_env \
        .execute_sql("INSERT INTO `Result` SELECT order_id, product_id, unit_price, quantity, MAX(discount) AS discount, "
                   "MAX(input_number) AS input_number, MAX(rowtime) AS rowtime "
                   "FROM order_details WHERE quantity > 35 "
                   "GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE), "
                   "order_id, product_id, unit_price, quantity ")
    # result.execute_insert("Result").wait()
    # tweets.group_by("a").select(tweets.a, tweets.b.count.alias('cnt')).execute_insert("Result").wait()
