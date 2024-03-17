# coding: utf8
import string

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import IntegerType, ArrayType, StringType, StructType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("udf").config("spark.sql.shuffle.partitions", 2).getOrCreate()
    spark_context = spark.sparkContext

    rdd = spark_context.parallelize([[1], [2], [3]])
    data_frame = rdd.toDF(["num"])
    struct_type = StructType().add("num", IntegerType(), nullable=True).add("letter", StringType(), nullable=True)


    def process_to_dict(data):
        return {"num": data, "letter": string.ascii_letters[data]}


    udf_letters = spark.udf.register("udf_letters", process_to_dict, struct_type)
    data_frame.createOrReplaceTempView("num_to_letters")
    spark.sql("select udf_letters(num) from num_to_letters").show(truncate=False)

    data_frame.select(udf_letters(data_frame["num"])).show(truncate=False)
