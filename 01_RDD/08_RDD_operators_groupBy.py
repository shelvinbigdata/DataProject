# coding: utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("operator_groupBy")
    spark_context = SparkContext(conf=spark_conf)
    rdd1 = spark_context.parallelize([("a", 1), ("b", 2), ("c", 1), ("b", 4), ("a", 7), ("a", 1), ("c", 1)]).filter(
        lambda t: t[1] % 2 == 1).distinct().groupBy(lambda t: t[0]).mapValues(lambda v: list(v))
    rdd2 = spark_context.parallelize([1, 2, 3, "hadoop"])
    # print(rdd2.union(rdd1).union(rdd2).collect())

    # join
    rdd3 = spark_context.parallelize(
        [(1001, "john"), (1001, "sam"), (1002, "shelvin"), (1003, "mike"), (1004, "lily")], 3)
    rdd4 = spark_context.parallelize([(1001, "dev"), (1001, "ba"), (1003, "tl")])
    print(rdd3.glom().collect())
    # print(rdd3.join(rdd4).collect())
    # print(rdd3.leftOuterJoin(rdd4).collect())
    # print(rdd3.rightOuterJoin(rdd4).collect())

    # intersection
    rdd5 = spark_context.parallelize([(1001, "john"), (1001, "sam"), (1002, "shelvin")])
    rdd6 = spark_context.parallelize([(1001, "john"), (1001, "david"), (1003, "shelvin")])
    # print(rdd5.intersection(rdd6).collect())
