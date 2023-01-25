import os
import sys
from pyspark.sql.types import *
PATH = "D:/书籍资料整理/ml-100k"

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
import matplotlib
import matplotlib.pyplot as plt

conf = SparkConf().setAppName("First Spark App").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def get_user_data():
    custom_schema = StructType([
        StructField("no", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("zipCode", StringType(), True)
    ])

    sql_context = SQLContext(sc)

    user_df = sql_context.read \
        .format('com.databricks.spark.csv') \
        .options(header='false', delimiter='|') \
        .load("%s/u.user" % PATH, schema = custom_schema)
    return user_df


def get_movie_data_df():
    custom_schema = StructType([
        StructField("no", StringType(), True),
        StructField("moviename", StringType(), True),
        StructField("date", StringType(), True),
        StructField("f1", StringType(), True), StructField("url", StringType(), True),
        StructField("f2", IntegerType(), True), StructField("f3", IntegerType(), True),
        StructField("f4", IntegerType(), True), StructField("f5", IntegerType(), True),
        StructField("f6", IntegerType(), True), StructField("f7", IntegerType(), True),
        StructField("f8", IntegerType(), True), StructField("f9", IntegerType(), True),
        StructField("f10", IntegerType(), True), StructField("f11", IntegerType(), True),
        StructField("f12", IntegerType(), True), StructField("f13", IntegerType(), True),
        StructField("f14", IntegerType(), True), StructField("f15", IntegerType(), True),
        StructField("f16", IntegerType(), True), StructField("f17", IntegerType(), True),
        StructField("f18", IntegerType(), True), StructField("f19", IntegerType(), True)
    ])



    sql_context = SQLContext(sc)

    movie_df = sql_context.read \
        .format('com.databricks.spark.csv') \
        .options(header='false', delimiter='|') \
        .load("%s/u.item" % PATH, schema = custom_schema)
    return movie_df


def get_movie_data():
    return sc.textFile("%s/ml-100k/u.item" % PATH)



def get_rating_data():
    return sc.textFile("%s/ml-100k/u.data" % PATH)


user_data = get_user_data()
print(user_data.head())

user_ages = user_data.select('age').collect()
user_ages_list = []

user_ages_len = len(user_ages)
for i in range(0, (user_ages_len - 1)):
    user_ages_list.append(user_ages[i].age)

plt.hist(user_ages_list, bins=20, color='lightblue', stacked=True)
fig = matplotlib.pyplot.gcf()
fig.set_size_inches(16, 10)
plt.show()
spark.stop()
sc.stop()