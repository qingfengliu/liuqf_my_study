from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np
from pyspark.ml.linalg import DenseVector

PATH = "D:/书籍资料整理/ml-100k"


def cosineSimilarity(vec1,vec2):
   return vec1.dot(vec2) / (vec1.norm(2) * vec2.norm(2))

def parseRating(data):
    fields = data.value.split("\t")
    return [int(fields[0]), int(fields[1]), float(fields[2]), int(fields[3])]

def parse_title(data):
   fields = data.value.split("|")
   return [int(fields[0]), str(fields[1])]

conf = SparkConf().setAppName("First Spark App").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.master("local[2]").appName("FeatureExtraction").getOrCreate()
ratings = spark.read.text(PATH+"/u.data").rdd.map(parseRating).toDF()

#修改列明的方式可能有很多种但是现在试到这种刚好好使
ratings = ratings.selectExpr("_1 as userID","_2 as movieID","_3 as rating","_4 as time")
#ratings.show()

training, test=ratings.randomSplit([0.8, 0.2])

#implicitPrefs=true用于隐式反馈
#coldStartStrategy 冷启动策略,暂时忽略冷启动数据.
#rank 秩 就是K
als_model=ALS(maxIter=5,regParam=0.01,userCol="userID",itemCol="movieID",ratingCol="rating",coldStartStrategy="drop")
model=als_model.fit(training)

##model.itemFactors 就是物品矩阵
# print(model.userFactors.count())
# print(model.itemFactors.count())

predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                predictionCol="prediction")

# coldStartStrategy=drop 才能正常显示,貌似数据集中有冷启动数据,待测试
# rmse = evaluator.evaluate(predictions)
# print("Root-mean-square error = " + str(rmse))


#print(predictions.printSchema())

users = ratings.select(als_model.getUserCol()).distinct().limit(3)

# 根据java和scala users类型 会被锁定 必须 是某些类型的值
userSubsetRecs = model.recommendForUserSubset(users, 10)
userSubsetRecs.show()


# 目前那个书中的函数不可用predict 预测用 recommendForUserSubset recommendForAllUsers
#取一个用户的推荐
userRecs = model.recommendForAllUsers(10)
subrecs=userRecs.where(userRecs.userID == 28).select("recommendations.movieID", "recommendations.rating").collect()
print(subrecs)

#这个很慢 先注释掉了
# user_subset = ratings.where(ratings.userID == 28)
# subrecs2=model.recommendForUserSubset(user_subset,10).select("recommendations.movieID", "recommendations.rating").first()
# print(subrecs2)
print('---------------------------------------------------------')
#用 余弦相似度来求物品相似度.  使用物品矩阵每一列 是一个物品向量
#model.itemFactors.show(truncate=False)

itemid=567
# pyspark.sql.dataframe
# itemFactor=model.itemFactors
temp=model.itemFactors

# temp=cosineSimilarity(itemFactor,itemFactor)
itemvector=DenseVector(temp.filter(temp.id.contains(itemid)).collect()[0]['features'])

#结果有一定随机性
sims=temp.rdd.map(lambda x:[str(x[0]),float(cosineSimilarity(DenseVector(x[1]),itemvector))]).toDF(['id','Similarity'])

sims=sims.orderBy(sims.Similarity.desc())

sims.show()
data_title = spark.read.text(PATH+"/u.item").rdd.map(parse_title).toDF()
data_title = data_title.selectExpr("_1 as movieid","_2 as title")

df=sims.join(data_title,on=sims.id == data_title.movieid).select(sims.id,data_title.movieid,data_title.title,sims.Similarity)

data=df.toPandas()
spark.stop()
sc.stop()
data.to_csv("D:/书籍资料整理/567相似度.csv",index=False)
