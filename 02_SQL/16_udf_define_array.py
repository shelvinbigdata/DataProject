# coding: utf8
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType, ArrayType, StringType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("udf").config("spark.sql.shuffle.partitions", 2).getOrCreate()
    spark_context = spark.sparkContext

    rdd = spark_context.parallelize([["hadoop spark hive"], ["hadoop flink hive java"]])
    data_frame = rdd.toDF(["line"])

    def split_line(data):
        return data.split(" ")


    split_udf = spark.udf.register("split_udf", split_line, ArrayType(StringType()))
    data_frame.select(split_udf(data_frame["line"])).show()

    data_frame.createOrReplaceTempView("lines")
    spark.sql("select split_udf(line) from lines").show(truncate=False)

    functions_udf = functions.udf(split_line, ArrayType(StringType()))
    data_frame.select(functions_udf(data_frame["line"])).show(truncate=False)

