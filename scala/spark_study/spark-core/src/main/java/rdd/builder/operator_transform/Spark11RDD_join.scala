package rdd.builder.operator_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11RDD_join {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd1=sc.makeRDD(List(
      ("a",1),("b",2),("c",3)
    ))
    val rdd2=sc.makeRDD(List(
      ("a",4),("b",5),("c",6)
    ))
    //inner join
    val joinrdd=rdd1.join(rdd2)
    joinrdd.collect().foreach(println)

    sc.stop()
  }
}
