# coding: utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("home_work")
    spark_context = SparkContext(conf=spark_conf)
    file_rdd = spark_context.textFile("../../test_data/apache.log")
    request_rdd = file_rdd.map(lambda line: line.split("\t"))
    request_rdd.persist(StorageLevel.DISK_ONLY)

    # PV
    print(request_rdd.count())

    # UV
    print(request_rdd.map(lambda ls: ls[0].split(" ")[1]).distinct().count())

    # IP
    print(request_rdd.map(lambda ls: ls[0].split()[0]).distinct().collect())

    # most popular page
    print(
        request_rdd.map(lambda ls: (ls[0].split()[4], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda t: t[1], False, 1)
        .first()[0])
