package liuqf.kafka.producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerAcks {
    public static void main(String[] args) {
        //0配置 连接集群
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"liuqf1:9092,liuqf2:9092");

        //指定Key和Value的序列化类型
        //StringSerializer.class.getName() 的值是org.apache.kafka.common.serialization.StringSerializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);

        //1 创建生产者对象
        // "" hello
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<>(properties);
        //2 发送数据
        kafkaProducer.send(new ProducerRecord<>("first","atguigu")); //发送一条数据
        for(int i=0;i<5;i++){
            kafkaProducer.send(new ProducerRecord<>("first","atguigu"+i)); //发送一条数据
        }

        //3 关闭资源
        kafkaProducer.close();
    }
}
