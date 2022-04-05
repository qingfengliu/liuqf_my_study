from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
from pyspark.sql import functions as F


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    df = spark.read.format("csv"). \
        schema("id INT,subject STRING,score INT"). \
        load("D:/program/stu_score.txt")

    df.createTempView("score")
    spark.sql("select *,avg(score) over() as avg_score from score")