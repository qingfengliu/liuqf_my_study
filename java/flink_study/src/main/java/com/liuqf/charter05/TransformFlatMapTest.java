package com.liuqf.charter05;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("user1", "url1", 1000L),
                new Event("user2", "url2", 2000L),
                new Event("user3", "url3", 3000L));
        stream.flatMap(new MyFlatMapFunction()).print("1");

        //2.使用lambda表达式
        //注意返回值类型并不是Collector,而是Collector里的String.
        //这里貌似声明没有匿名那么返回值也不能匿名
        stream.flatMap( (Event event,Collector<String> col) -> {
            if (event.user.equals("user1"))
                    col.collect(event.user);
            else if (event.user.equals("user2")) {
                col.collect(event.user);
                col.collect(event.url);
                col.collect(event.timestamp.toString());
            }

        }).returns(new TypeHint<String>() {}).print("2");
        env.execute();
    }
    public static class MyFlatMapFunction implements org.apache.flink.api.common.functions.FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
        }
    }
}
