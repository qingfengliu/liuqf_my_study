package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
    rdd.aggregateByKey(0)(
      (x,y)=>math.max(x,y),
      (x,y)=> x+y
    ).collect().foreach(println)

    sc.stop()
  }
}
