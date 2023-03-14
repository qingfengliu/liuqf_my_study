package com.liuqf.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.time.Duration;

public class UDFTest_ScalaFunction {
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
        tableEnv.createTemporarySystemFunction("myhash", MyHashFunction.class);

        //3.使用自定义函数
        tableEnv.executeSql("select order_id,myhash(order_id) from order1").print();
    }
    public static class MyHashFunction extends ScalarFunction{
        public int eval(String str){
            return str.hashCode();
        }
    }
}
