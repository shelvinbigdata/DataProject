# coding:utf8
from pyspark import SparkConf, SparkContext
import json
import os
from city_with_category_18 import city_with_category

os.environ["HADOOP_CONF_DIR"] = "/config/in/spark/env/sh"

if __name__ == '__main__':
    spark_conf = SparkConf().setAppName("rdd_operator_demo").setMaster("yarn")
    spark_conf.set("spark.submit.pyFiles", "city_with_category_18.py")
    spark_context = SparkContext(conf=spark_conf)
    json_rdd = spark_context.textFile("hadoop://node1:8020/warehouse/test_data/order.text").flatMap(
        lambda line: line.split("|"))
    result_rdd = (json_rdd.map(lambda json_str: json.loads(json_str))
                  .filter(lambda dict_ele: dict_ele["areaName"] == "北京")
                  .map(city_with_category)
                  .distinct())
    print(result_rdd.collect())
