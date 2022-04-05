from pyspark import SparkConf,SparkContext
import os
import json
import os

#是可以上传到yarn上的
if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test01")

    sc = SparkContext(conf=conf)

    rdd=sc.parallelize([1,2,3,4,5,7,8,9])
    print(rdd.takeOrdered(3))
    print(rdd.takeOrdered(3,lambda x:-x))
    sc.stop()
