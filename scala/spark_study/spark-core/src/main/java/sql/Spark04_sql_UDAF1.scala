package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}


object Spark04_sql_UDAF1 {
  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()

    val df=spark.read.json("datasets/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("ageAvg",functions.udaf(new MyAvgUDAF()))
    spark.sql("select * from user").show()
    spark.sql("select ageAvg(age) from user").show()


    spark.close()
  }
  /*
  自定义聚合函数类:计算年龄的平均值
  1.继承 org.apache.spark.sql.expressions.Aggregator 定义泛型
    IN:输入的数据类型 Long
    BUF:缓冲区数据类型
    OUT:输出的数据类型 Long
  2.重新方法 8个方法
  * */
  //
  case class Buff(var total:Long,var count:Long)
  class MyAvgUDAF extends Aggregator[Long,Buff,Long]{
    //初始值或者0值
    //缓冲区初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    //根据输入数据更新缓冲区数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total=buff.total+in
      buff.count = buff.count+1
      buff
    }

    //合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff2.total + buff2.total
      buff1.count = buff2.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total/buff.count
    }

    //缓冲区编码
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
