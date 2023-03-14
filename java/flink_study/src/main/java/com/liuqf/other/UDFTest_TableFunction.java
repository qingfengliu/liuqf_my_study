package com.liuqf.other;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import java.time.Duration;

public class UDFTest_TableFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv=StreamTableEnvironment.create(env);
        //来自App的支付日志

        DataStream<Tuple3<String,String,Long>> appstream = env.fromElements(
                Tuple3.of("order1_1","app",1000L),
                Tuple3.of("order2_2","app",2000L),
                Tuple3.of("order3_3","app",3500L),
                Tuple3.of("order4_4","app",4000L),
                Tuple3.of("order5_5_3","app",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );

        tableEnv.createTemporaryView("order1",appstream,"order_id,app,pay_time");
        //注册自定义函数
        tableEnv.createTemporarySystemFunction("mysplit", MySplit.class);
        tableEnv.executeSql("select order_id,word,length from order1," +
                "LATERAL TABLE(MySplit(order_id)) AS T(word,length)").print();
    }
    //实现自定义表函数
    public static class MySplit extends TableFunction<Tuple2<String,Integer>>{
        public void eval(String str){
            String[] split = str.split("_");
            for (String s : split) {
                collect(Tuple2.of(s,s.length()));
            }
        }
    }
}
