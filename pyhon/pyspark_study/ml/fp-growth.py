from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.fpm import FPGrowth
import pandas as pd


PATH = "D:/书籍资料整理/ml-100k"


def parseRating(data):
    fields = data.value.split("\t")
    return [int(fields[0]), int(fields[1]), float(fields[2]), int(fields[3])]

conf = SparkConf().setAppName("First Spark App").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.master("local[2]").appName("FeatureExtraction").getOrCreate()
ratings = spark.read.text(PATH+"/u.data").rdd.map(parseRating).toDF(["userID","movieID","rating","time"])
# ratings.show()

#collect_list应该是通过特别的方式访问到,可能记载在_functions_1_6_over_column中.
data=ratings.groupby('userID').agg(F.collect_list("movieID").alias('items'))

print('------------------------------------------------------------------------------')
#要更改数据格式
# +------------------------+
# |items                   |
# +------------------------+
# |[r, z, h, k, p]         |
# |[z, y, x, w, v, u, t, s]|
# |[s, x, o, n, r]         |
# |[x, z, y, m, t, s, q, e]|
# |[z]                     |
# |[x, z, y, r, q, t, p]   |
# +------------------------+


fp = FPGrowth(itemsCol='items',minSupport=0.2, minConfidence=0.7)
fpm = fp.fit(data)

fpm.setPredictionCol("newPrediction") #猜测是修改预测列明

#频繁模式
freq=fpm.freqItemsets.sort("items").toPandas()


#关联规则
asso=fpm.associationRules.sort("antecedent", "consequent").toPandas()


spark.stop()
sc.stop()

freq.to_csv("D:/书籍资料整理/关联模式.csv",index=False)
asso.to_csv("D:/书籍资料整理/频繁模式.csv",index=False)