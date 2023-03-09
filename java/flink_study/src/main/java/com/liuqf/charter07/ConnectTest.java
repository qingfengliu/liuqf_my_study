package com.liuqf.charter07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Long> stream2 = env.fromElements(4L,5L,6L,7L,8L);

        stream1.connect(stream2).map(
                new CoMapFunction<Integer, Long, String>() {
                    @Override
                    public String map1(Integer value) throws Exception {
                        return value.toString();
                    }

                    @Override
                    public String map2(Long value) throws Exception {
                        return value.toString();
                    }
                }
        ).print();
        env.execute();
    }
}
