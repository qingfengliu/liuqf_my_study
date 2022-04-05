from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext
    #基于RDD
    rdd = sc.textFile("D:/program/people.txt").map(lambda x:x.split(","))\
    .map(lambda x:(x[0],int(x[1])))

    df1=rdd.toDF(["name","age"])
    df1.printSchema()
    #打印df数据
    df1.show(20,False)