# coding: utf-8
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    sparkConf = SparkConf().setMaster("local[*]").setAppName("WordCount")
    sparkContext = SparkContext(conf=sparkConf)

    file_rdd = sparkContext.textFile("../data/input/words.txt")
    words_rdd = file_rdd.flatMap(lambda line: line.split(" "))
    words_with_one_rdd = words_rdd.map(lambda x: (x, 1))
    count_rdd = words_with_one_rdd.reduceByKey(lambda a, b: a + b)
    print(count_rdd.collect())
    