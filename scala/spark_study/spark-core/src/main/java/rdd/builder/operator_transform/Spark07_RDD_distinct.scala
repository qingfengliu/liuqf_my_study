package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_distinct {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(1,1,2,2,2,3,3,4,5,6,8,8),2)


    rdd.distinct().collect().foreach(println)
    sc.stop()
  }
}
