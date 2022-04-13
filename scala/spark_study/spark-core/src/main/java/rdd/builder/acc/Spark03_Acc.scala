package rdd.builder.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(1,2,3,4))

    //sum无法从executor返回
    var sum=0
    val sumacc=sc.longAccumulator("sum")
//    sc.doubleAccumulator()
    //少加:在转换算子中调用累加器,如果没有行动算子的话,那么不会执行
    rdd.map(x=>{
      sumacc.add(x)
      x
    })
    println(sumacc.value)

    //多加,累加器是全局共享的,调了两次.一般情况下累加器会放在行动算子中.
    rdd.collect()
    rdd.collect()

    sc.stop()
  }
}
