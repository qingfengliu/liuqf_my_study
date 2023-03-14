package com.liuqf.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import java.time.Duration;

public class UDFTest_AggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);
        //来自App的支付日志

        DataStream<Tuple3<String,String,Long>> appstream = env.fromElements(
                Tuple3.of("order1","app",1000L),
                Tuple3.of("order2","app",2000L),
                Tuple3.of("order3","app",3500L),
                Tuple3.of("order4","app",4000L),
                Tuple3.of("order5","app",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );

        tableEnv.createTemporaryView("order1",appstream,"order_id,app,pay_time");

        //注册自定义函数
        tableEnv.createTemporarySystemFunction("myweightedavg", MyWeightedAvg.class);
        //注册自定义函数


        //3.使用自定义函数
        tableEnv.executeSql("select order_id,myweightedavg(pay_time,1) as w_avg from order1 group by order_id").print();

    }
    //单独定义一个累加器
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }

    // 自定义函数实现加权平均值
    public static class MyWeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count == 0) {
                return null;
            }else
                return accumulator.sum / accumulator.count;
        }
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        public void accumulate(WeightedAvgAccumulator accumulator, Long value,Integer weight) {
            accumulator.sum += value * weight;
            accumulator.count += weight;
        }

    }
}
