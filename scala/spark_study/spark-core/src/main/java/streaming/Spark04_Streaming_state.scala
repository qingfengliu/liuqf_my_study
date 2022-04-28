package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random


object Spark04_Streaming_state {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("cp")
    //无状态数据操作,只对当前的采集周期内的数据进行处理.
    //某些场合下,需要保留数据的统计结果(状态),实现数据的汇总
    val datas=ssc.socketTextStream("liuqf1",9999)
    val wordToOne=datas.map((_,1))
    //根据key对数据状态进行更新
    // 传递参数中含有两个值
    // 第一个值表示相同的key的value数据
    // 第二个值表示缓冲区相同key的value值
    val state=wordToOne.updateStateByKey(
      (seq:Seq[Int],buff:Option[Int])=>{
        val newCount=buff.getOrElse(0)+seq.sum
        Option(newCount)

      }
    )
//    val wordToCount=wordToOne.reduceByKey(_+_)
    state.print()


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
