package com.liuqf.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class UDFTest_TableAggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);
        //来自App的支付日志

        DataStream<Tuple3<String,String,Long>> appstream = env.fromElements(
                Tuple3.of("order1","app",1000L),
                Tuple3.of("order2","app",2000L),
                Tuple3.of("order1","app",3000L),
                Tuple3.of("order1","app",2000L),
                Tuple3.of("order3","app",3500L),
                Tuple3.of("order4","app",4000L),
                Tuple3.of("order4","app",4500L),
                Tuple3.of("order5","app",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );

        tableEnv.createTemporaryView("order1",appstream,"order_id,app,pay_time.rowtime");

        //注册自定义函数
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);
        //注册自定义函数


        //3.使用自定义函数
        Table table = tableEnv.sqlQuery("select order_id,count(app) as cnt,window_start,window_end from TABLE (" +
                "   TUMBLE(TABLE order1,DESCRIPTOR(pay_time), INTERVAL '10' SECOND)" +
                ")" +
                "group by order_id,window_start,window_end");

        table.groupBy($("window_end")).flatAggregate(Expressions.call("Top2",$("cnt")).as("value","rank"))
                .select($("window_end"),$("value"),$("rank"))
                .execute()
                .print();

    }
    //单独定义一个累加器,包含了当前最大和第二大的数据
    public static class Top2Accumulator {
        public long max1;
        public long secondmax;
    }
    //实现一个自定义的表聚合
    public static class Top2 extends TableAggregateFunction<Tuple2<Long,Integer>,Top2Accumulator>{
        public void accumulate(Top2Accumulator accumulator,Long value){
            if(value>accumulator.max1){
                accumulator.secondmax=accumulator.max1;
                accumulator.max1=value;
            }else if(value>accumulator.secondmax){
                accumulator.secondmax=value;
            }
        }
        public void emitValue(Top2Accumulator accumulator,org.apache.flink.util.Collector<Tuple2<Long,Integer>> out){
            if (accumulator.max1!=Long.MIN_VALUE){
                out.collect(Tuple2.of(accumulator.max1,1));
            }
            if (accumulator.secondmax!=Long.MIN_VALUE){
                out.collect(Tuple2.of(accumulator.secondmax,2));
            }
        }
        public Top2Accumulator createAccumulator(){
            Top2Accumulator  top2Accumulator=new Top2Accumulator();
            top2Accumulator.max1=Long.MIN_VALUE;
            top2Accumulator.secondmax=Long.MIN_VALUE;
            return top2Accumulator;
        }

    }
}