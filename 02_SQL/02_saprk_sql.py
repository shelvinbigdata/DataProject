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
                  .schema( struct_type)
                  .option("encoding", "utf-8")
                  .load("../test_data/sql/u.data"))
    data_frame.createOrReplaceTempView("rank")
    # user avg rank
    spark.sql("""
        select user_id, round(avg(movie_rank), 2) as user_avg_rank from rank group by user_id order by user_avg_rank desc
        """).show()

    # movie avg rank
    spark.sql("""
        select movie_id, round(avg(movie_rank), 2) as movie_avg_rank from rank group by movie_id order by movie_avg_rank desc
        """).show()

    # count of movies which rank > avg rank
    spark.sql("""
        select count(distinct(movie_id)) from rank where movie_rank > (select avg(rank.movie_rank) as avg_rank from rank)
           """).show()

    # user avg rank for high rank movie
    mvp_user_id = spark.sql("""
             select user_id, count(movie_id) as count from rank 
                where movie_rank > 3 
                group by user_id 
                order by count desc limit 1
           """).first().user_id

    spark.sql("select round(avg(movie_rank), 2) from rank where user_id = {}".format(mvp_user_id)).show()

    # avg rank for every user
    avg_data_frame = spark.sql("""
        select user_id, round(avg(movie_rank), 2) as avg_rank from rank group by user_id order by avg_rank desc
    """)

    avg_data_frame.show()
    #
    # print(avg_data_frame.first())
    # print(avg_data_frame.sort(avg_data_frame.avg_rank.asc()).first())

    # rank top 10
    agg_data_frame = (data_frame.groupBy("movie_id").agg(
        functions.count("movie_id").alias("cnt"),
        functions.round(functions.avg("movie_rank"), 2).alias("avg_rank")))

    agg_data_frame.where("cnt > 100").sort(agg_data_frame.avg_rank.desc()).limit(10).show()

