package rdd.builder.persist
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_persist {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val list=List("Hello Scala","Hello Spark")
    val rdd= sc.makeRDD(list)
    val flatrdd = rdd.flatMap(_.split(" "))
    val maprdd = flatrdd.map(x=>{
      println("@@@@@@@")
      (x,1)
    })
    val reduceRDD=maprdd.reduceByKey(_+_)

    reduceRDD.collect().foreach(println)
    println("****************************")
    //RDD 不存储数据,这部分是重新执行的结果,需要将maprdd持久化
    val grouprdd=maprdd.groupByKey()
    grouprdd.collect().foreach(println)
    sc.stop()
  }
}
