package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(1,2,3,4),2)

    //mapPartitions可以以分区为单位进行数据转换操作
    //但是没有处理完的数据不会释放掉,会费内存.容易内存溢出
    val maprdd=rdd.mapPartitions(
      iter=>{
        println(">>>>>>>>>>>>")
        iter.map(_*2)   //这里是函数返回值,其实是一个迭代器
      }
    )

    maprdd.collect()
    sc.stop()
  }
}
