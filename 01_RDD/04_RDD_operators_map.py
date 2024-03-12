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
