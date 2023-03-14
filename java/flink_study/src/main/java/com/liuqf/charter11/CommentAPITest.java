package com.liuqf.charter11;

import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CommentAPITest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.定义环境配置来创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv1 = TableEnvironment.create(settings);

        // 批处理环境
        EnvironmentSettings settings2 = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);

        // 创建表
        // 不仅要引入flink-csv包，还要引入flink-sql-connector-files包
        String createDDL= "create table dingdan( insurant_cid_number string," +
                "splancode string, " +
                "applirelation_deal string," +
                "insurant_sex string," +
                "insurant_birthday_cid BIGINT," +
                "acceptdate BIGINT," +
                "policystatus string," +
                "pay_source string," +
                "cid_number_md5 string) " +
                "with ('connector'='filesystem'," +
                "'path' = 'D:\\泰康学习\\造数\\订单数据.csv'," +
                "'format' = 'csv'" +")";

        tableEnv1.executeSql(createDDL);

        //创建一张用于输出的表
        String createOutDDL= "create table outTable( insurant_cid_number string," +
                "splancode string, " +
                "applirelation_deal string," +
                "cid_number_md5 string) " +
                "with ('connector' = 'filesystem'," +
                "'path' = 'D:\\泰康学习\\输出'," +
                "'format' = 'csv'" +")";
        tableEnv1.executeSql(createOutDDL);

        //插入方式1
        Table table = tableEnv1.sqlQuery("select " +
                "splancode,applirelation_deal,cid_number_md5,cid_number_md5 " +
                "from dingdan");

        table.executeInsert("outTable");
//        tableEnv1.toChangelogStream(table).print();
        //插入方式2, 不需要再使用execute了
//        tableEnv1.executeSql("insert into outTable " +
//                "select splancode,applirelation_deal,cid_number_md5,cid_number_md5 " +
//                "from dingdan");
        //Table dingdan = tableEnv1.from("dingdan");
        // 将由datastream转换而来的表注册为临时表用tablenv.createTemporalTableFunction()方法


    }
}
