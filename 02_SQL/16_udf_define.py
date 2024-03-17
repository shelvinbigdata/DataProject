# coding: utf8
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("udf").config("spark.sql.shuffle.partitions", 2).getOrCreate()
    spark_context = spark.sparkContext

    rdd = spark_context.parallelize([1, 2, 3, 4, 5]).map(lambda x: [x])
    data_frame = rdd.toDF(["num"])

    # spark_session.udf.register() for SQL and DSL
    def num_ride_10(num):
        return num * 10

    udf_register = spark.udf.register("num_list", num_ride_10, IntegerType())

    data_frame.selectExpr("num_list(num)").show()

    data_frame.select(udf_register(data_frame["num"])).show()

    # pyspark.sql.functions.udf
    functions_udf = functions.udf(num_ride_10, IntegerType())
    data_frame.select(functions_udf(data_frame["num"])).show()
