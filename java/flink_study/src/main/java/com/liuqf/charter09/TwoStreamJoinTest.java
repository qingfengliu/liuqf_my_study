package com.liuqf.charter09;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TwoStreamJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //来自App的支付日志
        DataStream<Tuple3<String,String,Long>> stream1 = env.fromElements(
                Tuple3.of("order1","app",1000L),
                Tuple3.of("order2","app",2000L),
                Tuple3.of("order3","app",3500L),
                Tuple3.of("order4","app",4000L),
                Tuple3.of("order5","app",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );

        //来自微信的支付日志
        DataStream<Tuple3<String,String,Long>> stream2 = env.fromElements(
                Tuple3.of("order1","wechat",3000L),
                Tuple3.of("order2","wechat",4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );
        //自定义列表状态进行全外连接
        stream1.keyBy(data->data.f0)
                .connect(stream2.keyBy(data->data.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    //定义列表状态用于保存两个流中已经到达的所有数据
                    private ListState<Tuple2<String,Long>> listState1;
                    private ListState<Tuple2<String,Long>> listState2;
                    public void open(Configuration paramters) throws Exception{
                        listState1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("listState1", Types.TUPLE(Types.STRING,Types.LONG)));
                        listState2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("listState2", Types.TUPLE(Types.STRING,Types.LONG)));

                    }
                    @Override
                    public void processElement1(Tuple3<String, String, Long> left, Context ctx, Collector<String> out) throws Exception {
                        for (Tuple2<String, Long> right : listState2.get()) {
                            out.collect(left+"=>"+right);
                        }
                        listState1.add(Tuple2.of(left.f0,left.f2));

                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                        for (Tuple2<String, Long> left : listState1.get()) {
                            out.collect(left+"=>"+right);
                        }
                        listState2.add(Tuple2.of(right.f0,right.f2));
                    }
                })
                .print();
        env.execute();
    }
}
