package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(1,2,3,4),2)
    def groupFunction(num:Int):Int ={
      num % 2
    }
    val grouprdd=rdd.groupBy(groupFunction)
    grouprdd.collect().foreach(println)
    sc.stop()
  }
}
