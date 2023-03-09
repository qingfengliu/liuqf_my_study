package com.liuqf.charter06;

import com.liuqf.charter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000L);  //设置自动水位线间隔
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
        );
        //乱序流水线生成
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((event, timestamp) -> event.timestamp)
        );


        eventSingleOutputStreamOperator.keyBy(event -> event.user)
        .window(TumblingEventTimeWindows.of(Time.seconds(2)))
        //AggregateFunction<IN, ACC, OUT> acc是累加器
        .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, Tuple2<String, String>>() {
            public Tuple2<Long, Integer> createAccumulator() {
                return new Tuple2<>(0L, 0);
            }

            public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                //累加器的第一个值是时间戳的和,第二个值是个数
                return new Tuple2<>(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
            }

            public Tuple2<String, String> getResult(Tuple2<Long, Integer> accumulator) {
                return new Tuple2<>("平均时间戳", String.valueOf(accumulator.f0 / accumulator.f1));
            }

            public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
            }
        }).print();
        env.execute();
    }
}
