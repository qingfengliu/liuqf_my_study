package rdd.builder.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//自定义累加器
object Spark04_Acc {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List("hello","spark","hello"))

    //创建累加器对象,向Spark进行注册
    val wcacc=new MyAccumulator
    sc.register(wcacc,"WordCountAcc")
    rdd.foreach(
      word=>{
        wcacc.add(word)
      }
    )

    println(wcacc.value)
    sc.stop()
  }


  /*
  自定义累加器
  1.继承AccumulatorV2
    IN:累加器输入的数据类型
    OUT:累加器返回的数据类型
  * */
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
    private var wcMap=mutable.Map[String,Long]()

    //判断是否是初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    //复制一个累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    //重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: String): Unit = {
      val newcnt=wcMap.getOrElse(v,0L) + 1
      wcMap.update(v,newcnt)

    }

    //在driver端合并累加器的方法
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1=this.wcMap
      val map2=other.value
      map2.foreach{
        case (word,count)=> {
          val newcount=map1.getOrElse(word,0L)+count
          map1.update(word,newcount)
        }
      }
    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
