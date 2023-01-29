import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator,MulticlassClassificationEvaluator

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

#代码逐行注释如下：
#指定为csv格式
#指定第一行是字段名
#指定字段的分隔符是
#指定要加载的文件
PATH = "D:/书籍资料整理/stumbleupon"

row_df=spark.read.format('csv') \
        .option('header','true')    \
        .option('delimiter','\t')  \
        .load(PATH+'/train.tsv')


def replace_question(x):
    return ('0' if x=='?' else x)
replace_question=udf(replace_question)



df=row_df.select(
        ['url','alchemy_category']+
        [replace_question(col(column)).cast('double').alias(column)
        for column in row_df.columns[4:]]
        )

# df.show()


# 虽然不准备用pipeline但是还是需要 将 alchemy_category 转换成数字 其实用pipeline 更方便
stringIndexer=StringIndexer(inputCol='alchemy_category',
                           outputCol='alchemy_category_index')

model = stringIndexer.fit(df)
model.setHandleInvalid("error")
#alchemy_category_index 会加在最后一列并且不影响原列
df = model.transform(df)

encoder=OneHotEncoder(dropLast=False,
                     inputCol='alchemy_category_index',
                     outputCol='alchemy_category_vec')
model_encoder=encoder.fit(df)

#alchemy_category_vec 应该是以一个稀疏向量存在
df = model_encoder.transform(df)
# df.show()

assemblerInputs=['alchemy_category_score','avglinksize','commonlinkratio_1','commonlinkratio_2',
'commonlinkratio_3','commonlinkratio_4','compression_ratio','embed_ratio','framebased',
'frameTagRatio','hasDomainLink','html_ratio','image_ratio','is_news','lengthyLinkDomain',
'linkwordscore','news_front_page','non_markup_alphanum_characters','numberOfLinks','numwords_in_url',
'parametrizedLinkRatio','spelling_errors_ratio','alchemy_category_vec']

assembler=VectorAssembler(inputCols=assemblerInputs,
                         outputCol='features')

df = assembler.transform(df)
df=df.select('url','features','label')
df.show(truncate=False)

print('----------------------------------------------------')
train_df,test_df=df.randomSplit([0.7,0.3])

mlor = LogisticRegression(featuresCol='features',labelCol='label')
mlorModel = mlor.fit(train_df)

train_y=mlorModel.transform(train_df)
train_y.show()

train_y=train_y.select('url','label','prediction').toPandas()
train_df.cache()
test_df.cache()

test_y=mlorModel.transform(test_df)
test_y.show()

#这里在BinaryClassificationEvaluator只有auc和pr.
#这个多分类MulticlassClassificationEvaluator 有 但是不知道对不对先放在这里
evaluator = MulticlassClassificationEvaluator(predictionCol='prediction',
                labelCol='label',
                metricName='recallByLabel')
# evaluator=BinaryClassificationEvaluator(
#                 rawPredictionCol='prediction',
#                 labelCol='label',
#                 metricName='areaUnderROC')
auc=evaluator.evaluate(test_y)
print(auc)

test_y=test_y.select('url','label','prediction').toPandas()
train_df.cache()
test_df.cache()



#


spark.stop()
sc.stop()

train_y.to_csv("D:/书籍资料整理/逻辑回归结果_训练集.csv",index=False)
test_y.to_csv("D:/书籍资料整理/逻辑回归结果_测试集.csv",index=False)