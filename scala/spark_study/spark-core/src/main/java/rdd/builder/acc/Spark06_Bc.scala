package rdd.builder.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//自定义累加器
object Spark06_Bc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd1=sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val map=mutable.Map(("a",4),("b",5),("c",6))

    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map{
      case (w,c)=>{
        val l:Int = bc.value.getOrElse(w,0)
        (w,(c,l))
      }
    }


    sc.stop()
  }



}
