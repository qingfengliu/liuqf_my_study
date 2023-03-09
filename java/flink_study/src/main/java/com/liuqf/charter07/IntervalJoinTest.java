package com.liuqf.charter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String,Long>> stream1 = env.fromElements(
                Tuple2.of("a",1000L),
                Tuple2.of("b",2000L),
                Tuple2.of("a",3500L),
                Tuple2.of("d",4000L),
                Tuple2.of("e",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f1)
        );

        SingleOutputStreamOperator<Tuple2<String,Long>> stream2 = env.fromElements(
                Tuple2.of("b",1000L),  // 2s内的数据，不会被join
                Tuple2.of("a",2000L),
                Tuple2.of("d",6500L),
                Tuple2.of("b",7000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f1)
        );
        //仿佛stream1当做一个流,stream2看成了一个批
        stream1.keyBy(data->data.f0)
                .intervalJoin(stream2.keyBy(data->data.f0))
                .between(Time.seconds(-2),Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Tuple2<String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "-> " + right);
                    }
                })
                .print();
        env.execute();
    }
}
