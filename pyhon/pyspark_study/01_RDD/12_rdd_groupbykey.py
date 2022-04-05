from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    # conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    rdd1= sc.parallelize([('a',1),('a',1),('b',1),('b',1),('b',1)])
    rdd2=rdd1.groupByKey()

    #根据二元元组key进行关联
    print(rdd2.map(lambda x:(x[0],list(x[1]))).collect())

    sc.stop()