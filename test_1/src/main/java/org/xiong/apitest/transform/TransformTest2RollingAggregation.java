package org.xiong.apitest.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class TransformTest2RollingAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");

//        DataStream<SensorReading> dataStream = streamSource.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
//            }
//        });
        DataStream<SensorReading> dataStream = streamSource.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
                }
        );

        KeyedStream<SensorReading, String> stringKeyedStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });

        DataStream<SensorReading> temperature = stringKeyedStream.maxBy("temperature");
        temperature.print("maxBy");

        env.execute();
    }

}
