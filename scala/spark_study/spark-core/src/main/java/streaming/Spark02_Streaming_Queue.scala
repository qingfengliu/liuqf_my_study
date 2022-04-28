package streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object Spark02_Streaming_Queue {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数是批量处理的周期
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    // TODO 逻辑处理
    val rddQueue=new mutable.Queue[RDD[Int]]()
    val inputDStream=ssc.queueStream(rddQueue,oneAtATime = false)
    val mappedStream = inputDStream.map((_,1))
    val reducedStream = mappedStream.reduceByKey(_+_)
    reducedStream.print()

    ssc.start()
    for(i<-1 to 5){
      rddQueue+=ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
