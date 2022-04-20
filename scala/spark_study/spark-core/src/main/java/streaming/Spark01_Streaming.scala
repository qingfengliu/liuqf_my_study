package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Spark01_Streaming {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    // TODO 逻辑处理
    //获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.42.128", 9999)
    val words=lines.flatMap(_.split(" "))
    val wordToOne=words.map((_,1))
    val WordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    WordToCount.print()
    // TODO 关闭环境
//    ssc.stop()//由于采集器是长期执行的任务所以不能直接关闭
    //如果main方法执行完毕应用程序也会自动结束.
    //1.启动采集器
    ssc.start()
    //2.等待采集器关闭
    ssc.awaitTermination()
  }
}
