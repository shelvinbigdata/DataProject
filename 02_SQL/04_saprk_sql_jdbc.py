# coding: utf8
from pyspark.sql import SparkSession
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

    data_frame.write.mode("overwrite").format("jdbc") \
        .option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true") \
        .option("dbtable", "movie_data") \
        .option("user", "root") \
        .option("password", "12356") \
        .save()

    data_frame_read = (spark.read.format("jdbc")
                       .option("url", "jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true")
                       .option("dbtable", "movie_data")
                       .option("user", "root")
                       .option("password", "12356")
                       .load())
