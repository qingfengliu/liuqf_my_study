package com.liuqf.wc;
//无界流 ,
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Parameter;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //从参数中提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //2.从文件中读取数据
        DataStreamSource<String> lineDataSource = env.socketTextStream(host, port);

        //3.将每个单词进行切分,转换成二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordandone = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //4.按照单词进行分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordandone.keyBy(data->data.f0);

        //5.对每个分组进行聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();

        sum.print();

        //6.执行任务
        env.execute();
    }
}
