#搜索时间、用户ID、搜索内容、URL返回排名、用户点击顺序、用户点击的URL
from pyspark import SparkConf,SparkContext
from pyspark.storagelevel import StorageLevel
import os
import jieba
from search_rdd import context_jieba,filter_words,append_words,extract_user_and_word


if __name__=='__main__':

    os.environ["PYSPARK_PYTHON"] = '/usr/local/python3.8/bin/python3'
    os.environ["SPARK_HOME"] = '/usr/local/spark-3.2.1'

    conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    conf.set("spark.submit.pyFiles", "search_rdd.py")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("hdfs://liuqf1:9000/datasets/SogouQ.txt")

    # split_rdd会被多次使用所以需要缓存技术
    split_rdd=file_rdd.map(lambda x:x.split("\t"))
    #缓存
    split_rdd.persist(StorageLevel.DISK_ONLY)

    #用户搜索关键词分析
    #主要分析热点词
    # print(split_rdd.takeSample(True,3))

    context_rdd=split_rdd.map(lambda x:x[2])

    #
    words_rdd=context_rdd.flatMap(context_jieba)
    # print(words_rdd.collect())

    #数据处理,
    #院校 帮->院校帮
    #博学 谷->博学谷
    #传智播 客->传智播客
    filter_rdd=words_rdd.filter(filter_words)
    #关键词转换
    final_word_rdd=filter_rdd.map(append_words)
    #对单词进行分组,聚合排序求出前5
    result1=final_word_rdd.reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print("需求1结果:",result1)

    #TODO 2: 用户和关键词组合分析
    #1,我喜欢传智播客
    #1+我 1+喜欢 1+传智播客  那些用户搜索哪些词
    user_content_rdd=split_rdd.map(lambda x:(x[1],x[2]))
    #对用户搜索内容进行分词
    user_word_with_one_rdd=user_content_rdd.flatMap(extract_user_and_word)
    result2=user_word_with_one_rdd.reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print("需求2结果:",result2)

    #TODO 3:热门搜索时间段分析
    time_rdd=split_rdd.map(lambda x:x[0])

    #只保留小时
    hour_with_one_rdd=time_rdd.map(lambda x:(x.split(":")[0],1))
    result3 =hour_with_one_rdd.reduceByKey(lambda a,b:a+b).\
        sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print("需求3结果:", result3)