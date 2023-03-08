package com.liuqf.charter06;

import com.liuqf.charter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.DynamicEventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
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
                .assignTimestampsAndWatermarks( WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );
        stream.keyBy(data->data.user)
//                .countWindow(5)   //滚动计数窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))   //滚动时间窗口
//                .window(TumblingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))   //滚动时间窗口，带偏移量8:05,9:05为一个窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))      //滑动时间窗口,每5分钟滑动一次,每小时一个窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(5)))   //会话窗口
                .sum("count").print();

        env.execute();
    }
}
