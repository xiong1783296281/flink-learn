package org.xiong.apitest.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.mortbay.util.ajax.JSON;
import org.xiong.apitest.beans.SensorReading;

import java.util.*;

/**
 * @author: xiong
 * @create at 2020/12/31
 */
public class SinkTest3Els {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"3ab1d65cca35:9092");

        DataStreamSource<String> inputStream = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");
        DataStream<SensorReading> dataStream = inputStream.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
                }
        );

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.17.0.2",9200));
        httpHosts.add(new HttpHost("172.17.0.3",9200));
        httpHosts.add(new HttpHost("172.17.0.4",9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyElsSinkFunction()).build());

        env.execute();


    }

    // 自定义es写入操作
    public static class MyElsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            HashMap<String, String> reqData = new HashMap<>();
            reqData.put("id",sensorReading.getId());
            reqData.put("ts",sensorReading.getTimestamp().toString());
            reqData.put("temperature",sensorReading.getTemperature().toString());


            // 创建请求，发起写入es请求命令
            IndexRequest index = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(reqData);

            requestIndexer.add(index);

        }
    }

}
