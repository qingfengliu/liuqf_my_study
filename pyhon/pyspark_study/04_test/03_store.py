from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType
"""
需求1:各省销售额统计
需求2:Top3销售额省份中,有多少店铺打到过日销售额1000+
需求3:Top3省份中各省的平均单价.
需求4:Top3省份中各个省份的支付类型比例

receivable:订单金额
storeProvince:店铺省份
dateTS:订单销售日期
payType:支付类型
storeID:店铺ID

#写出结果到mysql
#写出结果到Hive库
"""



if __name__=='__main__':
    os.environ["PYSPARK_PYTHON"] = '/usr/local/python3.8/bin/python3'
    os.environ["SPARK_HOME"] = '/usr/local/spark-3.2.1'
    spark =SparkSession.builder.appName("test").\
        master("spark://liuqf1:7077").\
        config("spark.sql.warehouse.dir","hdfs://liuqf1:9000/user/hive/warehouse").\
        config("hive.metastore.uris","thrift://liuqf1:9083").\
        enableHiveSupport().\
        getOrCreate()
    # 省份缺失,删除
    # 列裁剪
    df=spark.read.format("json").load("hdfs://liuqf1:9000/datasets/mini.json").dropna(thresh=1,subset=['storeProvince']).\
        filter("storeProvince!='null'").filter("receivable<10000").\
        select('storeProvince','storeID','receivable','dateTS','payType')
    #TODO 1
    province_sale_df=df.groupby("storeProvince").sum("receivable").\
        withColumnRenamed("sum(receivable)","money").\
        withColumn("money",F.round("money",2)).orderBy("money",ascending=False)

    province_sale_df.show(truncate=False)
    #写入mysql暂时不写入
    # province_sale_df.write.mode("overwrite").format("jdbc").\
    #     option("url","jdbc:mysql://node1:3036/bigdata?useSSL=false&characterEncoding=utf8").\
    #     option("user","root").option("password","111111").option("encoding","utf-8").save()

    #需要已经配置好spark on hive
    #'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'
    # province_sale_df.write.mode("overwrite").saveAsTable("default.province_sale","parquet")

    #TODO 2
    top3_provice_df=province_sale_df.limit(3).select("storeProvince").withColumnRenamed("storeProvince","top3_province")

    top3_provice_df_joined=df.join(top3_provice_df,on=df['storeProvince']==top3_provice_df['top3_province'])
    top3_provice_df_joined.persist(StorageLevel.MEMORY_AND_DISK)

    province_hot_store_count_df =top3_provice_df_joined.groupby("storeProvince","storeID",
                                   F.unix_timestamp(df['dateTS'].substr(0,10),"yyyy-MM-dd").alias("day")).\
        sum("receivable").withColumnRenamed("sum(receivable)","money").\
        filter("money>1000").dropDuplicates(subset=['storeID']).groupby("storeProvince").count()

    province_hot_store_count_df.show()
    province_hot_store_count_df.write.mode("overwrite") .saveAsTable("default.province_hot_store_count", "parquet")

    #TODO 3
    top3_provice_avg_df=top3_provice_df_joined.groupby("storeProvince").avg("receivable").withColumnRenamed("avg(receivable)","money").\
        withColumn("money",F.round("money",2)).orderBy("money",ascending=False)

    top3_provice_avg_df.show(truncate=False)
    # top3_provice_avg_df.write.mode("overwrite") .saveAsTable("default.province_hot_order_avg", "parquet")

    #TODO 4
    top3_provice_df_joined.createTempView("province_pay")

    def udf_func(percent):
        return str(round(percent*100,2))+'%'
    #注册UDF
    my_udf=F.udf(udf_func,StringType())
    pay_type_df=spark.sql("""
        select storeProvince,payType,(count(payType)/total) as percent from
        (SELECT storeProvince,payType,count(1) over(partition by storeProvince) as total from province_pay) as sub
        group by storeProvince,payType,total
    """).withColumn("percent",my_udf("percent"))
    pay_type_df.show()
    pay_type_df.write.mode("overwrite").saveAsTable("default.pay_type", "parquet")
    top3_provice_df_joined.unpersist()
