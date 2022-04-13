package rdd.builder.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_persist {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    sc.setCheckpointDir("cp")
    val list=List("Hello Scala","Hello Spark")
    val rdd= sc.makeRDD(list)
    val flatrdd = rdd.flatMap(_.split(" "))
    val maprdd = flatrdd.map(x=>{
      println("@@@@@@@")
      (x,1)
    })
    maprdd.cache()
//    maprdd.checkpoint()
    println(maprdd.toDebugString)
    val reduceRDD=maprdd.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)
    println("****************************")
    println(maprdd.toDebugString)
    //RDD 不存储数据,这部分是重新执行的结果,需要将maprdd持久化
    val grouprdd=maprdd.groupByKey()
    grouprdd.collect().foreach(println)

    sc.stop()
  }
}
