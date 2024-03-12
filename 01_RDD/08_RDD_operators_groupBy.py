# coding: utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("operator_groupBy")
    spark_context = SparkContext(conf=spark_conf)
    print(spark_context.parallelize([("a", 1), ("b", 2), ("c", 1), ("b", 4), ("a", 7)]).filter(
        lambda t: t[1] % 2 == 1).groupBy(lambda t: t[0]).mapValues(
        lambda v: list(v)).collect())
