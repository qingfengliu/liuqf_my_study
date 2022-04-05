from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext
    #基于RDD
    rdd = sc.textFile("D:/program/people.txt").map(lambda x:x.split(","))\
    .map(lambda x:(x[0],int(x[1])))

    #nullable是否允许为空
    shema=StructType().add("name",StringType(),nullable=True).add("age",IntegerType(),nullable=False)
    df=spark.createDataFrame(rdd,schema=shema)
    df.printSchema()
    #打印df数据
    df.show(20,False)