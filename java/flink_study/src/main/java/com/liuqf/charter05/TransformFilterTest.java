package com.liuqf.charter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("user1", "url1", 1000L),
                new Event("user2", "url2", 2000L),
                new Event("user3", "url3", 3000L));

        //1.使用静态内部类
        stream.filter(new MyFilter()).print("1");

        //2.使用lambda表达式
        stream.filter(event -> event.user.equals("user2")).print("2");
        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("user1");
        }
    }
}
