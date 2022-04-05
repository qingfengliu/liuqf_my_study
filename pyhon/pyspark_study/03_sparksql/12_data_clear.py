from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd



if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    df = spark.read.format("csv").option("sep", ";").option("header", True).option("encoding", "utf-8"). \
        schema("name STRING,age INT,job STRING"). \
        load("D:/program/people.csv")

    df.drop_duplicates().show()
    df.drop_duplicates(['age','job']).show()

    df.dropna().show()
    df.fillna("N/A",subset=['job']).show()