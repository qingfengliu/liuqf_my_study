package com.liuqf.charter06;

import com.liuqf.charter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100L);  //设置自动水位线间隔
        DataStream<Event> stream = env.fromElements(
                new Event("Alice", "url1", 1000L),
                new Event("Bob", "./prod?id=100", 2000L),
                new Event("who", "url3", 3000L),
                new Event("Alice", "./prod?id=10", 3500L),
                new Event("Bob", "./prod?id=1", 2500L),
                new Event("who", "./prod?id=23", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Alice", "./prod?id=22", 2300L),
                new Event("Tom", "./prod?id=33", 3300L))
                //有序流的水位线生成
//                .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
//                )
//                //乱序流的时间。重点告诉什么流,告诉最大延迟时间,告诉如何提取时间戳
                .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        env.execute();
    }
}
