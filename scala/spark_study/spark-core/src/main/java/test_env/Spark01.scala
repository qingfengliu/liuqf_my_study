package test_env

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//agent.log 数据格式为时间戳,省份,城市,用户,广告,中间字段使用空格分隔.
//统计出每个省份广告被点击的Top3
// xxxxx 河北 张家口 张三 A -> 河北 A -> ((河北,A),1) -聚合 >((河北,A),5) -排序> (河北,(A,5))

object Spark01 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    //获取原始数据
    val file_rdd= sc.textFile("D:\\packages\\agent.log")
    //转换数据结构方便统计 直接转换成 ((河北,A),1)
    val rdd1=file_rdd.map(x=>{
      val y=x.split(" ")
      ((y(1),y(4)),1)
    })
//    rdd1.collect().foreach(println)
    //聚合
    val reducerdd=rdd1.reduceByKey(_+_)
    val newmaprdd=reducerdd.map{
      case ((prv,ad),sum) =>{
        (prv,(ad,sum))
      }
    }

    val grouprdd: RDD[(String, Iterable[(String, Int)])] = newmaprdd.groupByKey()

    //将聚合后的结构进行转换((河北,A),5) -> (河北,(A,5))

    //将转换后的数据进行分组(河北,[(A,5),(B,6)])

    //降序排序取前三
    val resultrdd=grouprdd.mapValues(
      iter =>{
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resultrdd.collect().foreach(println)
    sc.stop()
  }

}
