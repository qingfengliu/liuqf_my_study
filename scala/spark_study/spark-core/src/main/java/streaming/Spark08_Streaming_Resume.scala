package streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


object Spark08_Streaming_Resume {
  def main(args: Array[String]): Unit = {
    //检查点
    val ssc=StreamingContext.getActiveOrCreate("cp",()=>{
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
      ssc
    })
    ssc.checkpoint("cp")

    ssc.start()
    ssc.awaitTermination()



    // TODO 关闭环境
    ssc.start()
    ssc.awaitTermination()

  }
}
