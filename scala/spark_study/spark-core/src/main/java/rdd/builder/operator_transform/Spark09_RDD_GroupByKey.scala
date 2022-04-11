package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark09_RDD_GroupByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)),2)
    rdd.groupByKey().collect().foreach(println)
    println("------------------------")
    rdd.groupBy(_._1).collect().foreach(println)

    sc.stop()
  }
}
