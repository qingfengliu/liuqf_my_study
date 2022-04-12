package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark10_RDD_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)
    rdd.aggregateByKey(0)(
      (x,y)=>math.max(x,y),
      (x,y)=> x+y
    ).collect().foreach(println)
    println("--------------------------------")
    val rdd1=sc.makeRDD(List(
      ("a",1),("a",2),("b",3),
      ("b",4),("b",5),("a",6)
    ))

    //声明初始值为(0,0)就相当于告诉我们我们要求两个值,这里是和Python的agg差不多可以求多个聚合
    val newrdd:RDD[(String,(Int,Int))] =rdd1.aggregateByKey((0,0))(
      (t,v)=>{
        //这里t1,是从初始值来的(0,0)

        (t._1+v,t._2+1)
      },
      {
        (t1,t2)=>{
          (t1._1+t2._1,t1._2+t2._2)
        }
      }
    )
    val resultrdd=newrdd.mapValues{
      case (num,cnt)=>{
        num/cnt
      }
    }
    resultrdd.collect().foreach(println)
    println("--------------------------------")
    val newrdd2:RDD[(String,(Int,Int))] =rdd1.combineByKey(
      v=>(v,1),
      (t:(Int,Int),v)=>{
        //这里t1,是从初始值来的(0,0)

        (t._1+v,t._2+1)
      },
      {
        (t1:(Int,Int),t2:(Int,Int))=>{
          (t1._1+t2._1,t1._2+t2._2)
        }
      }
    )
    val resultrdd2=newrdd2.mapValues{
      case (num,cnt)=>{
        num/cnt
      }
    }
    resultrdd2.collect().foreach(println)
    sc.stop()
  }
}
