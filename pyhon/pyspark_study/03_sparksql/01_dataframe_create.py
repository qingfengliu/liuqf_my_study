from pyspark.sql import SparkSession


if __name__=='__main__':
    spark =SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc=spark.sparkContext
    #基于RDD
    rdd = sc.textFile("D:/program/people.txt").map(lambda x:x.split(","))\
    .map(lambda x:(x[0],int(x[1])))

    #构建DataFrame
    df=spark.createDataFrame(rdd,schema=['name','age'])
    #打印表结构
    df.printSchema()
    #打印df数据
    df.show(20,False)
    df.createTempView("people")
    spark.sql("select * from people where age<30").show()