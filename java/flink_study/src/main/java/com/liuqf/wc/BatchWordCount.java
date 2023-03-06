package com.liuqf.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("D:\\泰康学习\\造数\\word.txt");

        //3.将每个单词进行切分,转换成二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordandone = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.按照单词进行分组
        AggregateOperator<Tuple2<String, Long>> sum = wordandone.groupBy(0)
                //5.对每个分组进行聚合
                .sum(1);
        sum.print();
    }
}
