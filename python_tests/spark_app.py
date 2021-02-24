from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, TimestampType, DataType
from pyspark.sql.functions import window, current_timestamp


def foreach_batch_function(df, epoch_id):
    df.show()


def spark_request():
    spark = SparkSession \
        .builder \
        .appName("StructuredNetworkWordCount") \
        .getOrCreate()

    user_schema = StructType()\
        .add("order_id", "integer")\
        .add("product_id", "integer")\
        .add("unit_price", "float")\
        .add("quantity", "integer")\
        .add("discount", "float")\
        .add("number", "integer")

    csv_df = spark \
        .readStream \
        .option("sep", ",") \
        .schema(user_schema) \
        .option('maxFilesPerTrigger', '1')\
        .csv("/home/sriram/code/spark-3.0.1-bin-hadoop3.2/input/")

    # .option('includeTimestamp', 'true') \
    # Generate running word count

    time_df = csv_df.withColumn("time_column", current_timestamp()).withWatermark("time_column", "5 seconds")

    quantities = time_df.groupBy(
        window(time_df.time_column, '20 seconds', '10 seconds')
    ).sum("quantity")

    # selection = csvDF.limit(10)\
    #     .withWatermark("timestamp", "1 minutes") \
    #     .groupBy(csvDF.order_id, csvDF.product_id, csvDF.number,
    #                                     csvDF.unit_price, csvDF.quantity, csvDF.timestamp)\
    #     .sum("discount")

    query = quantities \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .outputMode("complete") \
        .format("console") \
        .start()

        # .foreachBatch(foreach_batch_function) \

    query.awaitTermination()
