from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
from pyspark.sql import functions as F


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext

    rdd=sc.parallelize([1,2,3,4,5,6,7]).map(lambda x:[x])

    df=rdd.toDF(["num"])
    def num_ride_10(num):
        return num*10

    udf2=spark.udf.register("udf1",num_ride_10,IntegerType())

    #SQL风格
    df.selectExpr("udf1(num)").show()

    df.select(udf2(df['num'])).show()

    #
    udf3=F.udf(num_ride_10,IntegerType())
    df.select(udf3(df['num'])).show()
