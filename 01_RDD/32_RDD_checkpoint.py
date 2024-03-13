#coding: utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel


if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("rdd_cache")
    spark_context = SparkContext(conf=spark_conf)
    spark_context.setCheckpointDir("hdfs://node1:8020/warehouse/output/checkpoint")
    rdd1 = spark_context.textFile("../test_data/input/words.txt")
    rdd2 = rdd1.flatMap(lambda line: line.split(" "))
    rdd3 = rdd2.map(lambda word: (word, 1))
    rdd3.checkpoint()
    rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
    # print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda v: sum(v))
    print(rdd6.collect())
    rdd3.unpersist()
