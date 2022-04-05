from pyspark import SparkConf,SparkContext
import os
import json
import os

#是可以上传到yarn上的
if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test01")

    sc = SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9,10],2)
    acmlt=sc.accumulator(0)


    def map_func(data):
        global acmlt
        acmlt+=1

    rdd2=rdd.map(map_func)
    rdd2.collect()

    rdd3=rdd2.map(lambda x:x)
    # rdd2.cache()   #cache会解决这个问题
    rdd3.collect()
    #rdd2这里被重构执行,然而global acmlt是没有释放的
    print(acmlt)

    sc.stop()