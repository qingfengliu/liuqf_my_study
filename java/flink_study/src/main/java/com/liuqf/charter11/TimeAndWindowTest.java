package com.liuqf.charter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TimeAndWindowTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createDDL= "create table clicks(" +
                "user_name string, " +
                "url string," +
                "ts BIGINT," +
                "et AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000,'yyyy-MM-dd HH:mm:ss') )," +
                "WATERMARK FOR et AS et - INTERVAL '1' SECOND" +
                ") with ('connector'='filesystem'," +
                "'path' = 'D:\\泰康学习\\造数\\clicks.csv'," +
                "'format' = 'csv'" +")";
        tableEnv.executeSql(createDDL);
        //旧版本窗口
//        tableEnv.sqlQuery("select * from clicks").execute().print();
//        tableEnv.sqlQuery("select user_name,count(1) as cnt," +
//                "TUMBLE_END(et,INTERVAL '10' SECOND) as entT" +
//                " from clicks group by user_name,TUMBLE(et,INTERVAL '10' SECOND)").execute().print();

        //滚动窗口
        tableEnv.sqlQuery("select user_name,count(1) as cnt," +
                "window_end as endT" +
                " from TABLE(" +
                " TUMBLE(TABLE clicks,DESCRIPTOR(et),INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user_name,window_end,window_start").execute().print();
        //滑动窗口
        tableEnv.sqlQuery("select user_name,count(1) as cnt," +
                "window_end as endT" +
                " from TABLE(" +
                " HOP(TABLE clicks,DESCRIPTOR(et),INTERVAL '10' SECOND,INTERVAL '5' SECOND)" +
                ")" +
                "GROUP BY user_name,window_end,window_start").execute().print();
        //会话窗口
        tableEnv.sqlQuery("select user_name,count(1) as cnt," +
                "window_end as endT" +
                " from TABLE(" +
                " SESSION(TABLE clicks,DESCRIPTOR(et),INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user_name,window_end,window_start").execute().print();
        //累积窗口
        //CUMULATE第三个参数为输出间隔时间
        tableEnv.sqlQuery("select user_name,count(1) as cnt," +
                "window_end as endT" +
                " from TABLE(" +
                " CUMULATE(TABLE clicks,DESCRIPTOR(et),INTERVAL '10' SECOND,INTERVAL '5' SECOND)" +
                ")" +
                "GROUP BY user_name,window_end,window_start").execute().print();


    }
}
