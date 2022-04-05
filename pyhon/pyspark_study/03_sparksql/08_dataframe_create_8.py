from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext


    df1=spark.read.format("parquet").load("D:/program/users.parquet")

    df1.printSchema()
    df1.show()