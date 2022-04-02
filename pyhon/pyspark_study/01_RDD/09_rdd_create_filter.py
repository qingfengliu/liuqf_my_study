from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    # conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    rdd= sc.parallelize([1,2,3,4,5,6,7,8,9])

    #或者直接lambda x:x%2==0
    print(rdd.filter(lambda x:True if x%2==0 else False).collect())
    sc.stop()