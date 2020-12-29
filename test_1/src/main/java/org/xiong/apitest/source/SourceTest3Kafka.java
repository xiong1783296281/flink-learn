package org.xiong.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class SourceTest3Kafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.17.0.2:9092");
//        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        dataStreamSource.print();

        env.execute();

    }


}
