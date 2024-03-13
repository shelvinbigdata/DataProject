# coding: utf8
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

from defs import extract_id_and_words, context_jieba, filter_words, append_words
from operator import add

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("search_log_analyse")
    spark_context = SparkContext(conf=spark_conf)
    file_rdd = spark_context.textFile("../../test_data/SogouQ.txt")

    split_rdd = file_rdd.map(lambda line: line.split("\t"))
    split_rdd.persist(StorageLevel.DISK_ONLY)

    # task1 top5 keywords
    context_rdd = split_rdd.map(lambda ls: ls[2])
    words_rdd = context_rdd.flatMap(context_jieba)
    filtered_rdd = words_rdd.filter(filter_words)
    final_words = filtered_rdd.map(append_words)
    sorted_rdd = final_words.reduceByKey(lambda a, b: a + b).sortBy(lambda t: t[1], False, 1)
    print(sorted_rdd.take(5))

    # task2 top5 id_keywords
    id_context_rdd = split_rdd.map(lambda ls: (ls[1], ls[2]))
    id_keyword_rdd = id_context_rdd.flatMap(extract_id_and_words)
    sorted_id_keyword_rdd = id_keyword_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda t: t[1], False, 1)
    print(sorted_id_keyword_rdd.take(5))

    # task3 top5 search hours
    time_rdd = split_rdd.map(lambda ls: ls[0])
    hour_rdd = time_rdd.map(lambda time_str: (time_str.split(":")[0], 1))
    sorted_hour_rdd = hour_rdd.reduceByKey(add).sortBy(lambda t: t[1], False, 1)
    print(sorted_hour_rdd.take(5))
