from pyspark import SparkConf,SparkContext


if __name__=='__main__':
    # conf = SparkConf().setMaster("local[*]").setAppName("wordcount")
    conf = SparkConf().setMaster("local[*]").setAppName("rdd")

    sc = SparkContext(conf=conf)

    rdd=sc.parallelize(["Hello Spark","Hello Spark"],2)
    print(rdd.map(lambda x:x.split(' ')).collect())
    print(rdd.flatMap(lambda x: x.split(' ')).collect())
    sc.stop()