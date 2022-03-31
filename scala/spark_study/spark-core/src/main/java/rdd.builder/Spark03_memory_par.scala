package rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_memory_par {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkconf)
    //并行,第二个参数时分区数量。如果不为则使用默认并行度,默认并行度可以设置,如果没设置则取最大核数.
    //spark.default.parallelism
    val rdd=sc.makeRDD(List(1,2,3,4),2)

    //将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

  }
}
