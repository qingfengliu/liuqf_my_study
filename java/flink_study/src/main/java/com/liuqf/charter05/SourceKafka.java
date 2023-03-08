package com.liuqf.charter05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Properties;

public class SourceKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties consumerPro = new Properties();
        consumerPro.setProperty("bootstrap.servers", "liuqf:9092");
        consumerPro.setProperty("group.id", "consumer-group");
        consumerPro.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerPro.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerPro.setProperty("auto.offset.reset", "latest");
        //kafka发送数据需要添加一个offset才能从最新的数据开始消费

        KafkaSource<String> testKafkaSource = KafkaSource.<String>builder()
                .setProperties(consumerPro)
                .setTopics("first")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // 从文件读取数据
        //FlinkKafkaConsumer 主题,反序列化器,配置
//        DataStream<String> dataStream = env.addSource( new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = env.fromSource(testKafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
