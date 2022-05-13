package com.atguigu.spark

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    //1. 初始化上下文环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val ssc = new StreamingContext(conf, Seconds(3))

    //2. 消费数据
    val kafkapara = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->"192.168.42.128:9092,192.168.42.129:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG->"test"
    )
    val KafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("first"), kafkapara))

    val valueDStream=KafkaDStream.map(record=>record.value())
    valueDStream.print()
    //3. 执行并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
