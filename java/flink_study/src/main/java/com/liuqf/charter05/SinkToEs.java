package com.liuqf.charter05;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.server.Request;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkToEs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("user1", "url1", 1000L),
                new Event("user2", "url2", 2000L),
                new Event("user3", "url3", 3000L));

        //定义hosts列表
        ArrayList<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("localhost", 9200));
        //写入ES

        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event element, RuntimeContext ctx, RequestIndexer indexer) {
                //TODO 写入ES
                HashMap<String, String> map = new HashMap<>();
                map.put(element.user, element.url);
                // 构建一个IndexRequest，用于发送http请求
                IndexRequest request = Requests.indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(map);
                indexer.add(request);
            }
        };

        stream.addSink(new ElasticsearchSink.Builder<>(hosts, elasticsearchSinkFunction).build());
        env.execute();
    }
}
