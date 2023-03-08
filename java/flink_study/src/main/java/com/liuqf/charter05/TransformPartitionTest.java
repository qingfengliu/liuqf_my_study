package com.liuqf.charter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Alice", "url1", 1000L),
                new Event("Bob", "./prod?id=100", 2000L),
                new Event("who", "url3", 3000L),
                new Event("Alice", "./prod?id=10", 3200L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./home", 3500L),
                new Event("who", "./home", 3800L));
        //1.随机分区
        //每个数据随机分区到某个分区,但是遵循均匀分布
//        stream.shuffle().print().setParallelism(4);
        //2.轮询分区
        // 第一个数据分配到第一个分区,第二个数据分配到第二个分区,以此类推.
//        stream.rebalance().print().setParallelism(4);

        //3.rescale分区
        // 将下游的分区分组,然后数据在各自分组中轮询分配.当然上游数据也需要分组
        // 比如上游分区2,下游分区4,那么上游的每个分区会被分配到下游相应的2个分区中

        // 首先将stream分成两个分区,基数发送到分区0,偶数发送到分区1
        env.addSource(new RichParallelSourceFunction<Integer>() {
            public void run(SourceContext<Integer> ctx) throws Exception{
                for (int i=1;i<=6; i++){
                    if (i%2== getRuntimeContext().getIndexOfThisSubtask()){
                        ctx.collect(i);
                    }
                }
            }

            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);

        //全局分区
        stream.global().print().setParallelism(4); //分区设多少都没用了 ,都是1个分区.
        //4.自定义分区
        env.fromElements(1,2,3,4,5,6,7,8,9,10).partitionCustom(new Partitioner<Integer>() {
            public int partition(Integer key, int numPartitions) {
                return key%2;
                //key%numPartitions 相当于轮询分区
            }
        }, new KeySelector<Integer, Integer>() {
            public Integer getKey(Integer value) throws Exception {
                return value;
            }
        }).print().setParallelism(4);
//        stream.rescale().print().setParallelism(4);

        env.execute();
    }
}
