package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(1,2,3,4))

    val maprdd=rdd.map(_*2)
    maprdd.collect().foreach(println)
    sc.stop()
  }
}
