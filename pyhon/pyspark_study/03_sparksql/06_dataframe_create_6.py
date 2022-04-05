from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    #默认将一整行认为是一列,默认列名为value

    df=spark.read.format("json").load("D:/program/people.json")

    df.printSchema()
    df.show()

    df1=spark.read.format("csv").option("seq",";").option("header",True).option("encoding","uft-8").\
        schema("name STRING,age INT,job STRING").\
        load("D:/program/people.csv")

    df1.printSchema()
    df1.show()