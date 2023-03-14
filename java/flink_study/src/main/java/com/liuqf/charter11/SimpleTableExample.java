package com.liuqf.charter11;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取得到datastream
        DataStream<Tuple3<String,String,Long>> appstream = env.fromElements(
                Tuple3.of("order1","app",1000L),
                Tuple3.of("order2","app",2000L),
                Tuple3.of("order3","app",3500L),
                Tuple3.of("order4","app",4000L),
                Tuple3.of("order5","app",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );

        //2.将datastream转换成table
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table1 = tableEnv.fromDataStream(appstream);
        //tableEnv.createTemporaryView("dingdan",table1);
        // table1 修改列明
        Table table2 = table1.select($("f0").as("order_id"),$("f1").as("channel"),$("f2").as("ts"));
        tableEnv.sqlQuery("select * from " + table2+" where order_id='order1'").execute().print();

//        table2.execute().print();
//        env.execute();
    }
}
