from pyspark import SparkConf,SparkContext
import os
import json
import re

if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test02")
    sc= SparkContext(conf=conf)

    file_rdd=sc.textFile('../datasets/boradcast.txt')
    #特殊字符的定义
    abnormal_char =[",",".","!","#","$","%"]
    #abnormal_char包装成广播变量.
    broadcast=sc.broadcast(abnormal_char)
    #对特殊字符进行累加.
    acmlt=sc.accumulator(0)
    #去除空白行
    lines_rdd=file_rdd.filter(lambda line:line.strip())
    #去除前后的空格
    data_rdd=lines_rdd.map(lambda line:line.strip())

    #用正则表达式切分,按照正则表达式切分,因为空格分隔符某些单词之间是两个或多个空格.
    words_rdd=data_rdd.flatMap(lambda x:re.split("\s+",x))

    #过滤数据正常数据保留用于特殊计数,特殊字符过滤掉并计数
    def filter_func(data):
        global acmlt
        #取出广播变量中的特殊符号List
        abnormal_chars=broadcast.value
        if data in abnormal_chars:
            acmlt+=1
            return False
        else:
            return True
    normal_words_rdd=words_rdd.filter(filter_func)
    #正常单词计数
    result_rdd=normal_words_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
    print("正常单词计数结果:",result_rdd.collect())
    print("特殊字符数量:",acmlt)