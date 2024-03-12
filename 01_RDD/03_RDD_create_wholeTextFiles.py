# coding: utf8
from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("RDD_wholeTextFiles")
    spark_context = SparkContext(conf=spark_conf)
    whole_text_files_rdd = spark_context.wholeTextFiles("../data/test_data/tiny_files")
    print("files content: ", whole_text_files_rdd.map(lambda text: text[1]).collect())
