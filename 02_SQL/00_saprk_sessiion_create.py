# coding: utf8
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("create_spark_session").getOrCreate()
    data_frame = spark.read.csv("../test_data/stu_score.txt", sep=',', header=False)
    data_frame_to_df = data_frame.toDF("id", "name", "score")
    # data_frame_to_df.printSchema()
    # data_frame_to_df.show()

    # SQL
    # data_frame_to_df.createTempView("score")
    # spark.sql("""
    #     select * from score where name = "语文" limit 5
    # """).show()

    # DSL
    data_frame_to_df.where("name = '语文'").limit(5).show()
