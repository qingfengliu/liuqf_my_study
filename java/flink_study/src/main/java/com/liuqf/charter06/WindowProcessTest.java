package com.liuqf.charter06;

import com.liuqf.charter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;

public class WindowProcessTest {

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
                new Event("Tom", "./prod?id=33", 3300L)
        ).assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
          .withTimestampAssigner((event, timestamp) -> event.timestamp)
        );
        stream.keyBy(data->true)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new UvCountByWindow()).print();
        env.execute();
    }

    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        public void process(Boolean s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            HashSet<String> userSet = new HashSet<>();
            for (Event element : elements) {
                userSet.add(element.user);
            }
            Integer uv= userSet.size();
            Long windowStart = context.window().getStart();
            Long windowEnd = context.window().getEnd();
            out.collect("uv: " + uv + ",window: [" + windowStart + "," + windowEnd + "]");
        }
    }
}
