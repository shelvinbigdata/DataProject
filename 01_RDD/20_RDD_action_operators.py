# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("rdd_operator_demo")
    spark_context = SparkContext(conf=spark_conf)
    rdd = spark_context.parallelize([1, 2, 3, 4, 5, 6], 3)
    # print(rdd.takeSample(False, 3))
    print(rdd.getNumPartitions())
    rdd.saveAsTextFile("../test_data/input/numbers")