from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,countDistinct,to_timestamp

spark=SparkSession.builder.appName("San Francisco xiaofang")    \
    .master("local").getOrCreate()
fire_schema=StructType([
    StructField('CallNumber',IntegerType(),True),
    StructField('UnitID',StringType(),True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AlarmDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', StringType(), True),
    StructField('ALSUnit', StringType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', StringType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', StringType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', StringType(), True)
])

sf_fire_file='D:/书籍资料整理/Spark快速大数据分析2E-随书代码包/chapter3/data/sf-fire-calls.csv'
fire_df=spark.read.csv(sf_fire_file,header=True,schema=fire_schema)
fire_df.show()

#保存代码,这里就不演示了,因为保存到文件也不是一个文件,
#parquet_path=''
#fire_df.write.format("csv").save(parquet_path)  #保存到文件
#parquet_table=''
#fire_df.write.format("csv").saveAsTable(parquet_table)  #保存到表
print('--------------------------------------------------------------------------------------------')
#筛选
few_fire_df=fire_df.select("IncidentNumber","AlarmDtTm","CallType").where(col("CallType")!="Medical Incident")
few_fire_df.show(5,truncate=False)
print('--------------------------------------------------------------------------------------------')
few_fire_df.select("CallType").where(col("CallType").isNotNull())   \
    .agg(countDistinct("CallType").alias("DistinctCallTypes")).show()

print('--------------------------------------------------------------------------------------------')
new_fire_df=fire_df.withColumnRenamed("Delay","ResponseDelayedinMins")
fire_ts_df=new_fire_df.withColumn("IncidentDate",to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate")   \
    .withColumn("OnWatchDate",to_timestamp(col("WatchDate"),"MM/dd/yyyy")).drop("WatchDate")    \
    .withColumn("AvailableDtTs",to_timestamp(col("AlarmDtTm"),"MM/dd/yyyy hh:mm:ss a")).drop("AlarmDtTm")
fire_ts_df.select("IncidentDate","OnWatchDate","AvailableDtTs").show()

#最常见的消防报警类型是什么
fire_ts_df.select("CallType").where(col("CallType").isNotNull()).groupBy("CallType").count()    \
    .orderBy("count",ascending=False).show()
print('--------------------------------------------------------------------------------------------')
#sum,avg等函数与python内置函数重名所以一般这样引用
#import pyspark.sql.functions as F
#fire_ts_df.select(F.sum("NumAlarms"),F.avg("ResponseDelayedinMins"))

#

spark.stop()