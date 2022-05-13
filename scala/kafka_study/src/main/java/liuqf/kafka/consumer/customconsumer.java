package liuqf.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class customconsumer {
    public static void main(String[] args) {
        //0 配置
        Properties properties= new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"liuqf1:9092,liuqf2:9092");
        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //配置消费者ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        //1. 创建一个消费者a
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<>(properties);
        //2 定义主题
        ArrayList<String> topics=new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);
        //3 消费数据
        while (true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String,String> consumerRecord:consumerRecords){
                System.out.println(consumerRecord);
            }
        }
    }
}
