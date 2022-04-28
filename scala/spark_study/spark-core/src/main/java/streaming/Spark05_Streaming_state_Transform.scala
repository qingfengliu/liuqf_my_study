package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object Spark05_Streaming_state_Transform {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //无状态数据操作,只对当前的采集周期内的数据进行处理.
    //某些场合下,需要保留数据的统计结果(状态),实现数据的汇总
    val lines=ssc.socketTextStream("liuqf1",9999)
    //transform方法可以将底层数据获取到
    //code:Driver端
    //Dstream功能不完善
    //需要代码周期性的执行
    val newDS: DStream[String] = lines.transform(
      rdd=>{
        //code: driver端,周期性执行(持续的采集一个周期执行一遍)
        rdd.map(
          //code:Executor端
          str=>str
        )
      }
    )
    //code:Driver端
    val newDS1=lines.map(
      data=>{
        //code:executor
        data
      }
    )

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
