package com.liuqf.charter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Alice", "url1", 1000L),
                new Event("Bob", "./prod?id=100", 2000L),
                new Event("who", "url3", 3000L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("TOM", "./prod?id=3", 3800L),
                new Event("Bob", "./prod?id=10", 4000L));
        StreamingFileSink<String> streamingfilesink = StreamingFileSink.<String>forRowFormat(new Path("D:\\data\\flink_study\\output"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(60 * 1000 * 15)            // withRolloverInterval如果文件大小超过15min 切换文件
                                .withInactivityInterval(60 * 1000 * 5)          // withInactivityInterval当前不活跃,切换文件 5min
                                .withMaxPartSize(1024 * 1024 * 1024)        // 文件最大长度 1GB
                                .build()
                ).build();
        stream.map(data->data.toString()).addSink(streamingfilesink);
        env.execute();
    }
}
