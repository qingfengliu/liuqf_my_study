package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Opearator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(1,2,3,4),1)

    //RDD的计算一个分区内的数据是一个一个执行逻辑
    //只有前面一个数据的逻辑全部执行完,才会执行下一个逻辑
    val maprdd=rdd.map(num =>
    {
      println(">>>>>>>> "+num)
      num
    })

    val maprdd1=maprdd.map(num =>
    {
      println("######### "+num)
      num
    })
    maprdd1.collect()
    sc.stop()
  }
}
