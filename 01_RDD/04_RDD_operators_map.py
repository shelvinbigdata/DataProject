# coding:utf8
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("operator_map")
    spark_context = SparkContext(conf=spark_conf)
    rdd = spark_context.parallelize([1, 2, 3, 4], 3)


    def multiplication(num):
        return num * 10


    print(rdd.map(multiplication).collect())
    print(rdd.map(lambda num: num * 10).collect())

    # flatmap
    parallelize_rdd = spark_context.parallelize(
        ["hello shelvin", "hadoop flink spark learn", "hello ning learn hadoop"])
    print(parallelize_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(
        lambda a, b: a + b).collect())
    # mapValues
    print(spark_context.parallelize([("hadoop", 1), ("hadoop", 2), ("flink", 3)]).mapValues(lambda v: v * 10).collect())
