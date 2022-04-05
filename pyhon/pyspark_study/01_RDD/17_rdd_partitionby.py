from pyspark import SparkConf,SparkContext
import os
import json
import os

#是可以上传到yarn上的
if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test01")

    sc = SparkContext(conf=conf)
    def process(k):
        if 'hadoop'==k or 'hello'==k:return 0
        if 'spark'==k:return 1
        return 2

    rdd=sc.parallelize([('hadoop',1),('spark',1),('hello',1),('flink',1),('hadoop',1),('spark',1)])
    print(rdd.partitionBy(3,process).glom().collect())
    sc.stop()
