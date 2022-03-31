package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
//ctrl +p 显示函数参数信息
object Spark01 {
  def main(args: Array[String]): Unit = {
    //准备环境
    //*是指核数
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //创建RDD
    //从内存中创建RDD,将内存中集合数据作为处理的数据源
    val seq=Seq[Int](1,2,3,4)
    //parallelize并行
//    val rdd=sc.parallelize(seq)
    val rdd=sc.makeRDD(seq)

    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }

}
