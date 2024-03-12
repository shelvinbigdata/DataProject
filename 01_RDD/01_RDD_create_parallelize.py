# coding: utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("parallelize_rdd")
    spark_context = SparkContext(conf=spark_conf)
    parallelize_rdd = spark_context.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    print("default partitions: ", parallelize_rdd.getNumPartitions())

    parallelize_rdd = spark_context.parallelize([1, 2, 3], 3)
    print("config partitions: ", parallelize_rdd.getNumPartitions())

    print("rdd content: ", parallelize_rdd.collect())
