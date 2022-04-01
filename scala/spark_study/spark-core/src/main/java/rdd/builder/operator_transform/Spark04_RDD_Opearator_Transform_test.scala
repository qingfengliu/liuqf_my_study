package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Opearator_Transform_test {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(
      List(1,2,3,4),2
    )
    val golmrdd=rdd.glom()
    val maxrdd=golmrdd.map(
      _.max
    )

    println(maxrdd.collect().sum)
    sc.stop()
  }
}
