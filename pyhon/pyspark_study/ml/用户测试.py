import numpy as np
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.ml.linalg import DenseVector

def cosineSimilarity(vec1,vec2):
   return vec1.dot(vec2) / (vec1.norm(2) * vec2.norm(2))

# print(cosineSimilarity(DenseVector([1,2,3]),DenseVector([1,2,3])))

# print(pyspark.__version__)

conf = SparkConf().setAppName("First Spark App").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.master("local[2]").appName("FeatureExtraction").getOrCreate()
value = [('Alice', [1,2,3]), ('Bob', [2,3,4])]
value2= [('Alice', [1,2,4]), ('Bob3', [2,3,2])]
df = spark.createDataFrame(value, ['name', 'age'])
df2 = spark.createDataFrame(value2, ['name', 'age'])

tmp=DenseVector([1,2,3])
tmp2=df.rdd.map(lambda x:[str(x[0]),float(cosineSimilarity(DenseVector(x[1]),tmp))]).toDF(['name','Similarity'])
tmp2=tmp2.orderBy(tmp2.Similarity.asc())
tmp3=tmp2.toPandas()

df3=df.join(df2,on=df.name == df2.name).select(df.name, df.age,df2.age.alias('age2'))
# df3.show()
# print(cosineSimilarity(tmp,tmp))
# print(type(tmp3))
# print(tmp3)
PATH = "D:/书籍资料整理/ml-100k"

def parse_title(data):
   fields = data.value.split("|")
   return [int(fields[0]), str(fields[1])]

data_title = spark.read.text(PATH+"/u.item").rdd.map(parse_title).toDF()
data_title = data_title.selectExpr("_1 as movieid","_2 as title")

data_title.show()


spark.stop()
sc.stop()