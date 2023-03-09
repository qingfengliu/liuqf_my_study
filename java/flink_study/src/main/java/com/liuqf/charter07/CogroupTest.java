package com.liuqf.charter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class CogroupTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String,Long>> stream1 = env.fromElements(
                Tuple2.of("a",1000L),
                Tuple2.of("b",2000L),
                Tuple2.of("c",3500L),
                Tuple2.of("d",4000L),
                Tuple2.of("e",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f1)
        );

        SingleOutputStreamOperator<Tuple2<String,Long>> stream2 = env.fromElements(
                Tuple2.of("a",2000L),
                Tuple2.of("b",4000L),  // 2s内的数据，不会被join
                Tuple2.of("a",4500L),
                Tuple2.of("b",6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f1)
        );

        stream1.coGroup(stream2)
                .where(data->data.f0)
                .equalTo(data->data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))// 2s内的数据，才会被join
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> first, Iterable<Tuple2<String, Long>> second, org.apache.flink.util.Collector<String> out) throws Exception {
                        for (Tuple2<String, Long> f : first) {
                            for (Tuple2<String, Long> s : second) {
                                out.collect(f + "=> " + s);
                            }
                        }
                    }
                }).print();
        env.execute();
    }
}
