package liuqf.kafka.producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallbackPartition {
    public static void main(String[] args) throws InterruptedException {
        //0配置 连接集群
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"liuqf1:9092,liuqf2:9092");

        //指定Key和Value的序列化类型
        //StringSerializer.class.getName() 的值是org.apache.kafka.common.serialization.StringSerializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //关联自定义分区器
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"liuqf.kafka.producer.MyPartitioner");

        //1 创建生产者对象
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<>(properties);
        //2 发送数据
        kafkaProducer.send(new ProducerRecord<>("first","atguigu")); //发送一条数据
        for(int i=0;i<500;i++){
            //
            kafkaProducer.send(new ProducerRecord<>("first","hello" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){
                        System.out.println("主题:"+recordMetadata.topic()+",分区:"+recordMetadata.partition());
                    }
                }
            }); //发送一条数据
            Thread.sleep(1);
        }

        //3 关闭资源
        kafkaProducer.close();
    }
}
