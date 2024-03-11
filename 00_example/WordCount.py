# coding: utf-8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    sparkConf = SparkConf().setAppName("WordCount")
    sparkContext = SparkContext(conf=sparkConf)

    file_rdd = sparkContext.textFile("hdfs://node1:8020/warehouse/tablespace/managed/hive")
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))
    count_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)
    print(count_rdd.collect())
    