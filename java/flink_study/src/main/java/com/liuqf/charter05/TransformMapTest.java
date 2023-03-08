package com.liuqf.charter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    //1.使用静态内部类
        DataStreamSource<Event> stream = env.fromElements(
                new Event("user1", "url1", 1000L),
                new Event("user2", "url2", 2000L));
        stream.map(new MyMapFunction()).print("1");

        //2.使用匿名类
        stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        }).print("2");

        //3.使用lambda表达式
        stream.map(event -> event.user).print("3");
        env.execute();
    }
    //自定义MapFunction

    public static class MyMapFunction implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }

}
