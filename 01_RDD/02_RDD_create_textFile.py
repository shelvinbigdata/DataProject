# coding: utf8
from pyspark import SparkContext, SparkConf
if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("RDD_textFile")
    spark_context = SparkContext(conf=spark_conf)
    text_file_rdd = spark_context.textFile("../data/input/words.txt")
    print("default partitions for text_file_rdd: ", text_file_rdd.getNumPartitions())
    print("text_file_rdd content: ", text_file_rdd.collect())

    text_file_rdd_2 = spark_context.textFile("../data/input", 13)
    print("partitions for text_file_rdd_2: ", text_file_rdd_2.getNumPartitions())

    text_file_rdd_3 = spark_context.textFile("../data/input/words.txt", 100)
    print("partitions for text_file_rdd_3: ", text_file_rdd_3.getNumPartitions())

    text_file_rdd_hdfs = spark_context.textFile("hdfs://node1:8020/warehouse/input/words.txt")
