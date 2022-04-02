from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    # os.environ['PYSPARK_PYTHON'] = "/usr/local/python3.8/bin/python3"
    conf=SparkConf().setAppName("wordcount")
    sc=SparkContext(conf=conf)

    file_rdd=sc.textFile('hdfs://liuqf1:9000/datasets/wordcount.txt')
    words_rdd=file_rdd.flatMap(lambda x:x.split(' '))

    words_with_one_rdd=words_rdd.map(lambda x:(x,1))
    result=words_with_one_rdd.reduceByKey(lambda a,b:a + b)
    print(result.collect())
