package rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//ctrl +p 显示函数参数信息
object Spark01 {
  def main(args: Array[String]): Unit = {
    //准备环境
    //*是指核数
    val master = "local"
    val sparkConf=new SparkConf().setMaster(master).setAppName("WordCount")
    val sc=new SparkContext(sparkConf)


    wordcount(sc)
    sc.stop()
  }

  def wordcount(sc: SparkContext):Unit={
    val rdd=sc.makeRDD(List("Hello Scala","Hello Spark"))
    val words=rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    var wordcount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  def wordcount2(sc: SparkContext):Unit={
    val rdd=sc.makeRDD(List("Hello Scala","Hello Spark"))
    val words=rdd.flatMap(_.split(" "))
    val wordOne=words.map((_,1))
    val group = wordOne.groupByKey()
    var wordcount= group.mapValues(iter => iter.size)
  }

  def wordcount3(sc: SparkContext):Unit={
    val rdd=sc.makeRDD(List("Hello Scala","Hello Spark"))
    val words=rdd.flatMap(_.split(" "))
    val wordOne=words.map((_,1))
    val wordcount = wordOne.reduceByKey(_+_)
  }

  def wordcount4(sc: SparkContext):Unit={
    val rdd=sc.makeRDD(List("Hello Scala","Hello Spark"))
    val words=rdd.flatMap(_.split(" "))
    val wordOne=words.map((_,1))
    val wordcount = wordOne.combineByKey(
      v=>v,
      (x:Int,y)=>x+y,
      (x:Int,y:Int)=>x+y
    )
  }
  def wordcount5(sc: SparkContext):Unit={
    val rdd=sc.makeRDD(List("Hello Scala","Hello Spark"))
    val words=rdd.flatMap(_.split(" "))
    val wordOne=words.map((_,1))
    val wordcount = wordOne.countByKey()

  }
}
