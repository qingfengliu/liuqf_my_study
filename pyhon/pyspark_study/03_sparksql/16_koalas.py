from pyspark.sql import SparkSession
import os
import pandas as pd
import pyspark.pandas as ps
import numpy as np


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

    dates = pd.date_range('20130101', periods=6)
    pdf = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list('ABCD'))
    kdf = ps.from_pandas(pdf)
    print(kdf.head(5))
    print(kdf.describe())
    print(kdf['A'].max())