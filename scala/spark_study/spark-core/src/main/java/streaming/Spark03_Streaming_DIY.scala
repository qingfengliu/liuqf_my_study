package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random


object Spark03_Streaming_DIY {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    // TODO 逻辑处理
    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyRecevice())
    messageDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
  /*
  继承Receiver,定义泛型
  重新方法
  * */
  class MyRecevice extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flg=true
    //启动采集器
    override def onStart(): Unit = {
      new Thread(
         new Runnable  {
          override def run(): Unit = {
            while (flg){
              val message="采集的数据为:"+new Random().nextInt(10).toString
              store(message)
              Thread.sleep(500)
            }
          }
        }
      ).start()
    }
    //关闭采集器
    override def onStop(): Unit = {
      flg=false
    }
  }
}
