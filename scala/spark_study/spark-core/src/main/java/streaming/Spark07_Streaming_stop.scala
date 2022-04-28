package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


object Spark07_Streaming_stop {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    // TODO 逻辑处理
    //获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("liuqf1", 9999)
    val words=lines.flatMap(_.split(" "))
    val wordToOne=words.map((_,1))
    val WordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //不会出现时间戳
    WordToCount.print()

    // TODO 关闭环境

    ssc.start()

    new Thread(
      new Runnable {
        override def run(): Unit = {
          //优雅的关闭
          //计算节点不再接收新的数据,将现有的数据处理完毕然后关闭
          //如果想要关闭采集器,需要创建一个新的线程
          //可以创建一个标志 来判断是否需要关闭.
          //可以在Hdfs文件系统里设置一个文件标志.
//          while (true) {
//            val state:StreamingContextState=ssc.getState()
//            if (state==StreamingContextState.ACTIVE) {
//              ssc.stop(true, true)
//            }
//            Thread.sleep(5000)
//          }
          Thread.sleep(5000)
          val state:StreamingContextState=ssc.getState()
          if (state==StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
        }
      }
    ).start()
    //2.等待采集器关闭
    ssc.awaitTermination()

  }
}
