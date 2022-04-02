from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    # conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    rdd= sc.parallelize([1,2,3,4,5,6,7,8,9])
    print('默认分区数:',rdd.getNumPartitions())

    rdd=sc.parallelize([1,2,3],3)
    #collect将RDD每个分区的数据都发送到Driver中,形成一个Python List对象
    print(rdd.collect())
    sc.stop()