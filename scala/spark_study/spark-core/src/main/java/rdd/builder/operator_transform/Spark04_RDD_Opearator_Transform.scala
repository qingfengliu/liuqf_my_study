package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(
      List(List(1,2),List(3,4))
    )
    val flatrdd=rdd.flatMap(
      list=>{
        list
      }
    )



    flatrdd.collect().foreach(println)
    sc.stop()
  }
}
