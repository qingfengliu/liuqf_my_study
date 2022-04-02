from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    # conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    rdd= sc.parallelize([('a',1),('b',1),('a',1),('c',1)])
    print(rdd.mapValues(lambda x: x*10).collect())
    sc.stop()