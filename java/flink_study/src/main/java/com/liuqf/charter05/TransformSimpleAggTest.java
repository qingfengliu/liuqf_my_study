package com.liuqf.charter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Alice", "url1", 1000L),
                new Event("Bob", "./prod?id=100", 2000L),
                new Event("who", "url3", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L));

        //匿名类求最大值
        stream.keyBy(new KeySelector<Event, String>() {
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max");
        //max与maxby 在非key和聚合数据的处理上有区别.max取的是第一条,maxby取的是最大的聚合数据哪条
        stream.keyBy(e->e.user).maxBy("timestamp").print("max2");


        env.execute();

    }
}
