from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd



if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    rdd=sc.textFile('../datasets/wordcount.txt').flatMap(lambda x: x.split(' ')).map(lambda x:[x])
    df=rdd.toDF(["word"])
    df.createTempView("words")

    spark.sql("select word,count(*) as cnt from words group by word").show()

    #DSL风格处理
    df=spark.read.format("text").load('../datasets/wordcount.txt')
    #withColumn
    df2=df.withColumn("value",F.explode(F.split(df['value'],' ')))
    df2.groupby("value").count().withColumnRenamed("count","cnt").orderBy("cnt",ascending=False).show()