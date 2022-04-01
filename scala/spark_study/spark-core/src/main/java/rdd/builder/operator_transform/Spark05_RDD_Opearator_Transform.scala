package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(
      List(List(1,2),3,List(4,5))
    )
    val flatrdd=rdd.flatMap(
      s=>{
        s match {
          case list: List[_]=>list
          case dat=>List(dat)
        }
      }
    )



    flatrdd.collect().foreach(println)
    sc.stop()
  }
}
