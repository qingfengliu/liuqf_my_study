package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object Spark06_Streaming_window {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val lines=ssc.socketTextStream("liuqf1",9999)
    val wordToOne=lines.map((_,1))
    //窗口范围应该是采集周期的整数倍
    // 默认一个采集周期移动,会出现重复数据计算,可以改变滑动的幅度
    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6),Seconds(6))
    val wordToCount=windowDS.reduceByKey(_+_)
    wordToCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
