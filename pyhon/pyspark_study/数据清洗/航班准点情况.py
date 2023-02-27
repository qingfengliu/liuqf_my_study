from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("hangban").master("local").getOrCreate()

csv_file="D:/书籍资料整理/Spark快速大数据分析2E-随书代码包/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

#读取数据并创建临时表
#推断表结构,如果数据量较大,最好手动指定表结构
df=spark.read.format("csv") \
    .option("inferSchema","true")   \
    .option("header","true")    \
    .load(csv_file)
df.createTempView("us_delay_flights_tbl")

#schema="date string,delay int,distince int,origin string,destination string"
spark.sql("""select distance,origin,destination from us_delay_flights_tbl 
            distance>1000 order by distance desc""").show()




