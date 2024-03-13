# coding: utf8

from pyspark import SparkConf, SparkContext


def map_character(data):
    global accumulator
    for word in data:
        if word in broadcast.value:
            accumulator += 1
            return False
        else:
            return True


if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("broadcast_accumulator")
    spark_context = SparkContext(conf=spark_conf)

    special_characters = [",", ".", "!", "#", "$", "%"]
    broadcast = spark_context.broadcast(special_characters)
    accumulator = spark_context.accumulator(0)

    file_rdd = spark_context.textFile("../test_data/accumulator_broadcast_data.txt")
    words_rdd = file_rdd.flatMap(lambda line: line.split())
    normal_rdd = words_rdd.filter(map_character).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    print("normal words: ", normal_rdd.collect())
    print("special characters count: ", accumulator)
