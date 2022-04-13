package rdd.builder.operator_transform.Part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

//自定义分区规则
object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(
      ("nba","xxxxxxxxxxx"),
      ("cba","xxxxxxxxxxx"),
      ("wnba","xxxxxxxxxxx"),
      ("nba","xxxxxxxxxxx"),
    ),3)
    var partrdd: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partrdd.saveAsTextFile("output")
    sc.stop()
  }
  //自定义分区器

  class MyPartitioner extends Partitioner{
    override def numPartitions:Int =3
    //根据数据的key值 返回数据的分区索引,从0开始
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "wnba"=>1
        case _=>2
      }

    }
  }
}
