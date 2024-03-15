# coding: utf8
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark_sql").getOrCreate()
    struct_type = (StructType().add("user_id", StringType())
                   .add("movie_id", StringType())
                   .add("movie_rank", IntegerType())
                   .add("time", StringType()))

    data_frame = (spark.read.format("csv")
                  .option("sep", "\t")
                  .option("header", False)
                  .schema(struct_type)
                  .option("encoding", "utf-8")
                  .load("../test_data/sql/u.data"))

    data_frame.write.mode("overwrite").format("csv").option("sep", ";").option("header", True).save(
        "../test_data/write/csv")

    (data_frame.select(functions.concat_ws("---", "user_id", "movie_id", "movie_rank", "time"))
        .write.mode("overwrite").format("text").save("../test_data/write/text"))

    data_frame.write.mode("overwrite").format("json").save("../test_data/write/json")
    data_frame.write.format("parquet").save("../test_data/write/parquet")
