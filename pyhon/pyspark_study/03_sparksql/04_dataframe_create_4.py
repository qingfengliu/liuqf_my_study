from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext
    #基于pandas
    pdf=pd.DataFrame({"id":[1,2,3],"name":["张大仙","王晓晓","吕不韦"],"age":[11,21,11]})
    df=spark.createDataFrame(pdf)
    df.printSchema()
    #打印df数据
    df.show(20,False)