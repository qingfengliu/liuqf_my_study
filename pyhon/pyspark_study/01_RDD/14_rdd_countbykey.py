from pyspark import SparkConf,SparkContext
import os
import json
import os

#是可以上传到yarn上的
if __name__=='__main__':
    # os.environ['HADOOP_CONF_DIR'] = 'D:/program/hadoop-3.2.2/etc/hadoop'
    conf = SparkConf().setMaster("local[*]").setAppName("test01")

    sc = SparkContext(conf=conf)
    file_rdd=sc.textFile("../datasets/wordcount.txt")

    jsons_rdd=file_rdd.flatMap(lambda x:x.split(' '))
    dict_rdd=jsons_rdd.map(lambda x:(x,1))
    result=dict_rdd.countByValue()
    #保留北京的数据
    print(result)
    print(type(result))

    sc.stop()