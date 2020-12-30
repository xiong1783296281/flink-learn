package org.xiong.apitest.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.xiong.apitest.beans.SensorReading;

import java.util.Properties;

/**
 * @author: xiong
 * @create at 2020/12/30
 */
public class SinkTest2Redis {

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

        FlinkJedisPoolConfig con = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .setPassword("123456")
                .setDatabase(2).build();

        dataStream.addSink(new RedisSink<>(con, new MyRedisMapper()));

        env.execute();

    }

    // 自定义redismapper
    public static class MyRedisMapper implements RedisMapper<SensorReading>{

        // 定义保存数据到redis的命令，存成Hash表， hset sensor_temp id temperature
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemperature().toString();
        }
    }

}
