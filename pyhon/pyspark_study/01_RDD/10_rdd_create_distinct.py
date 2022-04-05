from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    # conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    rdd= sc.parallelize([1,2,3,4,5,5,1,2,3,2,3,5])

    #或者直接lambda x:x%2==0
    print(rdd.distinct().collect())
    rdd2 = sc.parallelize([('a',1),('a',1),('a',3)])
    print(rdd2.distinct().collect())
    sc.stop()