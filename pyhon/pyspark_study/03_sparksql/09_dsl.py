from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    df= spark.read.format("csv"). \
        schema("id INT,subject STRING,score INT"). \
        load("D:/program/stu_score.txt")

    df.select(['id','subject']).show()
    df.select('id', 'subject').show()

    id_column=df['id']
    sub_column=df['subject']
    df.select(id_column, sub_column).show()

    df.filter("score <90").show() #
    df.filter(df['score']<99).show()

    # df.groupby
    df.groupby(['subject']).count().show()

