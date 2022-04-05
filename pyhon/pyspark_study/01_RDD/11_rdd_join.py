from pyspark import SparkConf,SparkContext
import os

if __name__=='__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    # conf = SparkConf().setMaster("spark://liuqf1:7077").setAppName("wordcount")
    # conf = SparkConf().setAppName("wordcount")
    sc = SparkContext(conf=conf)

    rdd1= sc.parallelize([(1001,"zhangsan"),(1002,"lisi"),(1003,"wangwu"),(1004,"zhaoliu")])
    rdd2= sc.parallelize([(1001,"销售部"),(1002,"科技部")])

    #根据二元元组key进行关联
    print(rdd1.join(rdd2).collect())
    print(rdd1.leftOuterJoin(rdd2).collect())
    print(rdd1.rightOuterJoin(rdd2).collect())
    sc.stop()