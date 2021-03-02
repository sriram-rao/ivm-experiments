from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, window, current_timestamp, max as max_, to_date, date_format
from pyspark.sql.functions import split
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, TimestampType, DataType

user_schema = StructType() \
    .add("order_id", "integer") \
    .add("product_id", "integer") \
    .add("unit_price", "float") \
    .add("quantity", "integer") \
    .add("discount", "float") \
    .add("number", "integer")

orders_schema = StructType() \
    .add("order_id", "integer") \
    .add("customer_id", "string") \
    .add("employee_id", "integer") \
    .add("order_date", "date") \
    .add("required_date", "date") \
    .add("shipped_date", "date") \
    .add("ship_via", "integer") \
    .add("freight", "float") \
    .add("ship_name", "string") \
    .add("ship_address", "string") \
    .add("ship_city", "string") \
    .add("ship_region", "string") \
    .add("ship_postal_code", "string") \
    .add("ship_country", "string")


def foreach_batch_function(df, epoch_id):
    df.show()


def spark_request():
    spark = SparkSession \
        .builder \
        .appName("WindowQuery") \
        .getOrCreate()

    csv_df = spark \
        .readStream \
        .option("sep", ",") \
        .schema(user_schema) \
        .csv("/Users/sriramrao/code/spark-bin/input/")

    # .option('includeTimestamp', 'true') \

    time_df = csv_df.withColumn("time_column", current_timestamp()) \
        .withWatermark("time_column", "5 seconds")

    quantities = time_df.groupBy(
        window(time_df.time_column, '20 seconds', '10 seconds'),
        time_df.product_id
    ).sum("quantity") \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .select("window_start", "window_end", "product_id", "sum(quantity)") \
        .orderBy("window_start")

    # selection = time_df \
    #     .groupBy(time_df.product_id, time_df.time_column) \
    #     .sum("quantity")\
    #     .withColumn("time_string", date_format(col("time_column"), "yyyy-MM-dd HH:mm:ss"))\
    #     .select("product_id", "time_string", "sum(quantity)")\
    #     .orderBy("sum(quantity)", ascending=False)\
    #     .limit(10)

    query = quantities \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("complete") \
        .format("console") \
        .start()

    # .foreachBatch(foreach_batch_function) \
    # file_query = selection.writeStream \
    #     .format("csv") \
    #     .outputMode("append") \
    #     .option("path", "/Users/sriramrao/code/spark-bin/output") \
    #     .trigger(processingTime="10 seconds") \
    #     .option("checkpointLocation", "/Users/sriramrao/code/spark-bin/checkpoints") \
    #     .start()
    # .partitionBy("col") # if you need to partition

    query.awaitTermination()


def spark_join_request():
    spark = SparkSession \
        .builder \
        .appName("JoinQuery") \
        .getOrCreate()

    csv_df = spark \
        .readStream \
        .option("sep", ",") \
        .option("trigger", "15 seconds") \
        .schema(user_schema) \
        .csv("/Users/sriramrao/code/spark-bin/input/")

    orders_df = spark \
        .readStream \
        .option("sep", ",") \
        .option("trigger", "15 seconds") \
        .schema(orders_schema) \
        .csv("/Users/sriramrao/code/spark-bin/input2/")
    # .option('includeTimestamp', 'true') \

    # orders_df = orders_df \
    #     .withColumn("time_column2", current_timestamp()) \
    #     .withWatermark("time_column2", "5 seconds")

    time_df = csv_df.withColumn("time_column", current_timestamp()) \
        .withWatermark("time_column", "5 seconds") \
        .join(orders_df, 'order_id', 'inner') \
        .select('order_id', 'ship_country', 'employee_id', 'quantity', 'time_column')

    # quantities = time_df.groupBy(
    #     window(time_df.time_column, '20 seconds', '10 seconds'),
    #     time_df.product_id
    # ).sum("quantity") \
    #     .withColumn("window_start", col("window.start")) \
    #     .withColumn("window_end", col("window.end")) \
    #     .select("window_start", "window_end", "product_id", "sum(quantity)") \
    #     .orderBy("window_start")

    selection = time_df \
        .groupBy(time_df.ship_country,
                 window(time_df.time_column, '30 seconds', '15 seconds'))\
        .sum("quantity") \
        .withColumn("window_start", col("window.start")) \
        .withColumn("time_string", date_format(col("window_start"), "yyyy-MM-dd HH:mm:ss")) \
        .select("ship_country", "time_string", "sum(quantity)")
    # .orderBy("sum(quantity)", ascending=False)\

    query = selection \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .option("checkpointLocation", "/Users/sriramrao/code/spark-bin/checkpoints") \
        .outputMode("append") \
        .format("console") \
        .start()

    # .foreachBatch(foreach_batch_function) \
    # file_query = selection.writeStream \
    #     .format("csv") \
    #     .outputMode("append") \
    #     .option("path", "/Users/sriramrao/code/spark-bin/output") \
    #     .trigger(processingTime="10 seconds") \
    #     .option("checkpointLocation", "/Users/sriramrao/code/spark-bin/checkpoints") \
    #     .start()
    # .partitionBy("col") # if you need to partition

    query.awaitTermination()
