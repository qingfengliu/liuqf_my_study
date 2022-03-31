package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

//ctrl +p 显示函数参数信息
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //准备环境
    //*是指核数
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //创建RDD
    //从文件中创建RDD,将文件中数据作为处理的数据源
//    val rdd=sc.textFile("datasets/1.txt")
    //也可以是目录
    val rdd=sc.textFile("datasets")
    //还可以使用通配符
//    val rdd=sc.textFile("datasets/1*.txt")
    //还可以使用Hdfs路径
//  val rdd=sc.textFile("hdfs://xxx:8020/test.txt")
    rdd.collect().foreach(println)
    //关闭环境
    sc.stop()
  }

}
