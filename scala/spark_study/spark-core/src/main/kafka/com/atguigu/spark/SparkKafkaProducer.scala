package com.atguigu.spark
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

object SparkKafkaProducer {
  def main(args: Array[String]): Unit = {
    //0 配置信息
    val properies = new Properties()
    properies.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.42.128:9092,192.168.42.129:9092")
    properies.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    properies.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])
    //1 创建一个生产者
    val producer = new KafkaProducer[String, String](properies)

    //2 发送数据
    for(i<- 1 to 5){
      producer.send(new ProducerRecord[String,String]("first","atguigu"+i))
    }
    //3 关闭资源
    producer.close()
  }
}
