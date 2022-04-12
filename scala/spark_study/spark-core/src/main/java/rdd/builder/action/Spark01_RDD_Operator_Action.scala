package rdd.builder.action
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {

    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkconf)

    val rdd=sc.makeRDD(List(1,2,3,4))
    val i=rdd.reduce(_+_)
    println(i)
    //关闭环境

    //将不同分区的数据,按照分区顺序采集到Driver端形成数组
    val ins=rdd.collect()
    println(ins.mkString(","))

    val cnt=rdd.count()
    println(cnt)
    sc.stop()
  }
}
