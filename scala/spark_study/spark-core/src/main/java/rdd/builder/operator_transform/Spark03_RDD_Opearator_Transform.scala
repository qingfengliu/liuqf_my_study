package rdd.builder.operator_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Opearator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("Opearator")
    val sc=new SparkContext(sparkconf)
    val rdd=sc.makeRDD(List(1,2,3,4),2)

    //mapPartitions可以以分区为单位进行数据转换操作
    //但是没有处理完的数据不会释放掉,会费内存.容易内存溢出
    val maprdd=rdd.mapPartitionsWithIndex(
      (index,iter)=>{
        if(index==1){
          iter
        }else{
          Nil.iterator
        }
      }
    )

    maprdd.collect().foreach(println)
    sc.stop()
  }
}
