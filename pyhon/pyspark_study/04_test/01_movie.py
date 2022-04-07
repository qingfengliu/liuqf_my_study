#用户ID、电影ID、评分、时间
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import os
import pandas as pd
from pyspark.sql import functions as F
import pyspark.pandas as pp

if __name__=='__main__':
    os.environ["PYSPARK_PYTHON"] = '/usr/local/python3.8/bin/python3'
    os.environ["SPARK_HOME"] = '/usr/local/spark-3.2.1'
    spark =SparkSession.builder.appName("test").\
        master("spark://liuqf1:7077").\
        config("spark.sql.warehouse.dir","hdfs://liuqf1:9000/user/hive/warehouse").\
        config("hive.metastore.uris","thrift://liuqf1:9083").\
        enableHiveSupport().\
        getOrCreate()

    schema=StructType().add("user_id",StringType()).add("moive_id",StringType()).\
        add("rank",IntegerType()).add("ts",StringType())

    df= spark.read.format("csv").option("sep","\t").\
        option("header",False).schema(schema=schema).load("hdfs://liuqf1:9000/datasets/u.data")
    # pdf=df.toPandas().head()

    #用户平均分
    df.groupby("user_id").avg("rank").withColumnRenamed("avg(rank)","avg_rank").withColumn("avg_rank",F.round("avg_rank",2)).\
        orderBy("avg_rank",ascending=False).show()
    #电影平均分
    df.createTempView("movie")
    spark.sql("select moive_id,round(avg(rank),2) avg_rank from movie group by moive_id order by avg_rank desc").show()

    #用户平均分,最高分,最低分
    df.groupby("user_id").agg(
        F.round(F.avg("rank"),2).alias("avg_rank"),
        F.min("rank").alias("min_rank"),
        F.max("rank").alias("max_rank"),
    ).show()

    #查询评分超过100次的电影,的平均分排名top100
    df.groupby("movie_id").agg(
        F.count("movie_id").alias("cnt"),
        F.round(F.avg("rank"),2).alias("avg_rank")
    ).where("cnt > 100").orderBy("avg_rank",ascending=False).limit(10).show()