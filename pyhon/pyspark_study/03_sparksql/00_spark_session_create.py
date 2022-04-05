from pyspark.sql import SparkSession
import os
import json
import re

if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    #获取sparkContext
    sc=spark.sparkContext
    df=spark.read.csv("D:/program/stu_score.txt",sep=',',header=False)
    df2=df.toDF("id","name","score")
    df2.printSchema()
    df2.show()

    #sql风格
    df2.createTempView("score")
    spark.sql("select * from score where name='语文' limit 5").show()

    #DSL风格
    df2.where("name='语文'").limit(5).show()