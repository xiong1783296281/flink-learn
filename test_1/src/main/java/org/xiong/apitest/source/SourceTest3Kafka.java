package org.xiong.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

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
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"172.17.0.2:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group_lalala");
        // 反序列配置写死了 ByteArrayDeserializer.class
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("lalala", new SimpleStringSchema(), properties));

        dataStreamSource.print();

        env.execute();

    }


}
