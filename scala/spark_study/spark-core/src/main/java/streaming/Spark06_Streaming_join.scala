package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object Spark06_Streaming_join {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val data9999=ssc.socketTextStream("localhost",9999)
    val data8888=ssc.socketTextStream("localhost",8888)

    val map9999: DStream[(String, Int)] = data9999.map((_, 1))
    val map8888: DStream[(String, Int)] = data8888.map((_, 1))


    //DSjoin操作就是RDD的join
    val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)
    joinDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
