# coding: utf8
from pyspark.sql import SparkSession, functions

if __name__ == '__main__':
    spark = SparkSession.builder.appName("data_frame").getOrCreate()
    data_frame = spark.read.text("../test_data/words.txt")
    data_frame_with_column = data_frame.withColumn("value", functions.explode(functions.split(data_frame["value"], " ")))
    with_columns_renamed = data_frame_with_column.groupBy("value").count().withColumnsRenamed({"value": "words", "count": "cnt"})
    with_columns_renamed.sort(with_columns_renamed.cnt.asc()).show()


