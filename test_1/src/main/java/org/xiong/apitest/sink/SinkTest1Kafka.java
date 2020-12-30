package org.xiong.apitest.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.xiong.apitest.beans.SensorReading;

import java.util.Properties;

/**
 * @author: xiong
 * @create at 2020/12/30
 */
public class SinkTest1Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"3ab1d65cca35:9092");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));


//        DataStreamSource<String> inputStream = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");
        DataStream<String> dataStream = dataStreamSource.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2])).toString();
                }
        );

        dataStream.addSink(new FlinkKafkaProducer<String>("3ab1d65cca35:9092", "lalala", new SimpleStringSchema()));



        env.execute();
    }

}
