from pyspark import SparkConf,SparkContext
import os
import json
import os

#是可以上传到yarn上的
if __name__=='__main__':
    os.environ['HADOOP_CONF_DIR'] = 'D:/program/hadoop-3.2.2/etc/hadoop'
    conf = SparkConf().setMaster("yarn").setAppName("test01")
    # conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)
    file_rdd=sc.textFile("hdfs://localhost:9000/input/order.txt")

    jsons_rdd=file_rdd.flatMap(lambda x:x.split('|'))
    dict_rdd=jsons_rdd.map(lambda json_str:json.loads(json_str))

    #保留北京的数据
    beijing_rdd=dict_rdd.filter(lambda x:x['areaName']=='北京')
    #组合北京和商品类型形成新的字符串
    category_rdd=beijing_rdd.map(lambda x:x['areaName']+'_'+x['category'])

    #对结果集进行去重
    result_rdd=category_rdd.distinct()
    print(result_rdd.collect())

    sc.stop()