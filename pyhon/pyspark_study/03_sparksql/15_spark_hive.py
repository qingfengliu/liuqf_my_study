from pyspark.sql import SparkSession
import os



if __name__=='__main__':
    os.environ["PYSPARK_PYTHON"] = '/usr/local/python3.8/bin/python3'
    os.environ["SPARK_HOME"] = '/usr/local/spark-3.2.1'
    spark =SparkSession.builder.appName("test").\
        master("spark://liuqf1:7077").\
        config("spark.sql.warehouse.dir","hdfs://liuqf1:9000/user/hive/warehouse").\
        config("hive.metastore.uris","thrift://liuqf1:9083").\
        enableHiveSupport().\
        getOrCreate()
    sc=spark.sparkContext

    spark.sql("select * from student").show()