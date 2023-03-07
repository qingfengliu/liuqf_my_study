package com.liuqf.charter05;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;

//由于使用的Java版本太高 ,会报错module java.base does not “opens java.util“ to unnamed module
//解决办法:在VM options中添加--add-opens java.base/java.util=ALL-UNNAMED
//或者在pom.xml中添加
//<properties>
//    <maven.compiler.source>1.8</maven.compiler.source>
//    <maven.compiler.target>1.8</maven.compiler.target>
//    <maven.compiler.compilerVersion>1.8</maven.compiler.compilerVersion>

public class SourceText {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> stream1 = env.readTextFile("D:\\data\\flink_study\\clicks.txt");

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("user1", "url1", 1000L));
        events.add(new Event("user2", "url2", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        DataStreamSource<Event> stream3 = env.fromElements(new Event("user1", "url1", 1000L),
                new Event("user2", "url2", 2000L));
        stream1.print("1");
        stream2.print("2");
        stream3.print("3");
        env.execute();
    }
}
