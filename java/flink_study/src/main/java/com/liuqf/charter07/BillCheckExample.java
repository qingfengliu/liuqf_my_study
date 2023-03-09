package com.liuqf.charter07;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ValueRange;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        //TODO
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //来自App的支付日志
        DataStream<Tuple3<String,String,Long>> appstream = env.fromElements(
                Tuple3.of("order1","app",1000L),
                Tuple3.of("order2","app",2000L),
                Tuple3.of("order3","app",3500L),
                Tuple3.of("order4","app",4000L),
                Tuple3.of("order5","app",5000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f2)
        );

        //来自微信的支付日志
        DataStream<Tuple4<String,String,String,Long>> thirdpartStream = env.fromElements(
                Tuple4.of("order1","wechat","sucess",3000L),
                Tuple4.of("order2","wechat","sucess",4000L),
//                Tuple4.of("order3","wechat","sucess",3000L),
                Tuple4.of("order4","wechat","sucess",5000L),
                Tuple4.of("order5","wechat","sucess",6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner( (event,timestamp)->event.f3)
        );

        //检测同一支付单在两条流中是否匹配,不匹配就报警
        appstream.keyBy(data->data.f0)
                .connect(thirdpartStream.keyBy(data->data.f0))
                .process(new OrderMatchResult())
                .print();

        appstream.connect(thirdpartStream)
                .keyBy(data->data.f0,data->data.f0)
                .process(new OrderMatchResult())
                .print();
        env.execute();
    }
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>,Tuple4<String,String,String,Long>,String> {

        //状态变量用户保存已经到达的事件
        private ValueState<Tuple3<String,String,Long>> appEventState;
        private ValueState<Tuple4<String,String,String,Long>> thirdpartEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState= getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("appEvent", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
            );

            thirdpartEventState= getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdpartEvent", Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG))
            );
        }

        //和key有关
        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 来的是app event ,看另一条流中事件是否来过
            if (thirdpartEventState.value()!=null){
                //另一条流中有事件来过
                out.collect("match:"+value);
                thirdpartEventState.clear();
        }else {
                //另一条流中没有事件来过
                appEventState.update(value);
                //注册定时器 5秒后
                ctx.timerService().registerEventTimeTimer(value.f2+5000L);

            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 来的是thirdpart event ,看另一条流中事件是否来过
            if (appEventState.value()!=null){
                //另一条流中有事件来过
                out.collect("match:"+value);
                appEventState.clear();
            }else {
                //另一条流中没有事件来过
                thirdpartEventState.update(value);
                //注册定时器 5秒后
                ctx.timerService().registerEventTimeTimer(value.f3+5000L);

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，说明5秒内没有收到另一条流的事件
            if (appEventState.value()!=null){
                out.collect("no match:"+appEventState.value());
                appEventState.clear();
            }
            if (thirdpartEventState.value()!=null){
                out.collect("no match:"+thirdpartEventState.value());
                thirdpartEventState.clear();
            }
            appEventState.clear();
            thirdpartEventState.clear();
        }
    }
}
