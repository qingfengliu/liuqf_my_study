package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd1=sc.makeRDD(List(1,2,3,4),2)
    val rdd2=sc.makeRDD(List(3,4,5,6),2)

    //交集
    val rdd3=rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    //并集
    val rdd4=rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    //差集
    val rdd5=rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    //拉链
    val rdd6=rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))
    sc.stop()
  }
}
