package rdd.builder.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//自定义累加器
object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd1=sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val rdd2=sc.makeRDD(List(("a",4),("b",5),("c",6)))

    //join 会导致数据量几何增长,并且影响shuffle性能,不推荐使用.
    val joinrdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinrdd.collect().foreach(println)



    sc.stop()
  }



}
