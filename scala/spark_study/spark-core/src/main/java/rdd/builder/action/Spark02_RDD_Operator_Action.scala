package rdd.builder.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(1,2,3,4))

    val result=rdd.aggregate(0)(_+_,_+_)
    sc.stop()
  }
}
