package rdd.builder.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(1,2,3,4))

    //sum无法从executor返回
    var sum=0
    val sumacc=sc.longAccumulator("sum")
//    sc.doubleAccumulator()

    rdd.foreach(x=>{
      sumacc.add(x)
    })
    println(sumacc.value)
//    val i =rdd.reduce(_+_)
//    println(i)

    sc.stop()
  }
}
