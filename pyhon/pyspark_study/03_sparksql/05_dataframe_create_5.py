from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    #默认将一整行认为是一列,默认列名为value
    shema = StructType().add("data", StringType(), nullable=True)
    df=spark.read.format("text").schema(schema=shema).load("D:/program/people.txt")

    df.printSchema()