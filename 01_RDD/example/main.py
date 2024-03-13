# coding: utf8
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import context_jieba, filter_words, append_words

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("search_log_analyse")
    spark_context = SparkContext(conf=spark_conf)
    file_rdd = spark_context.textFile("../../test_data/SogouQ.txt")

    split_rdd = file_rdd.map(lambda line: line.split("\t"))
    split_rdd.persist(StorageLevel.DISK_ONLY)

    context_rdd = split_rdd.map(lambda ls: ls[2])
    words_rdd = context_rdd.flatMap(context_jieba)
    filtered_rdd = words_rdd.filter(filter_words)
    final_words = filtered_rdd.map(append_words)

    sorted_rdd = final_words.reduceByKey(lambda a, b: a + b).sortBy(lambda t: t[1], False, 1)
    print(sorted_rdd.take(5))
