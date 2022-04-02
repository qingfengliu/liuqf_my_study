from pyspark import SparkConf,SparkContext


if __name__=='__main__':
    # conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    conf = SparkConf().setMaster("local[*]").setAppName("rdd")

    sc = SparkContext(conf=conf)

    rdd=sc.parallelize([1,2,3,4,5],2)
    print(rdd.map(lambda x:x*10).collect())
    sc.stop()