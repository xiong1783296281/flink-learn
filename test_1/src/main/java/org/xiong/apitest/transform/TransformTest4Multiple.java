package org.xiong.apitest.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.xiong.apitest.beans.SensorReading;

import java.util.Collections;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class TransformTest4Multiple {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");

        DataStream<SensorReading> dataStream = streamSource.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
                }
        );

        // 分流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemperature()>30 ? Collections.singletonList("high"):Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high", "low");

        // 合并流，前提是两个流的泛型类型相同
        DataStream<SensorReading> unionStream = highStream.union(lowStream);

//        highStream.print("high");
//        lowStream.print("low");
//        allStream.print("all");

        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        // 合并流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowStream);
        DataStream<Tuple3<String, Double, String>> tempWarning = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Tuple3<String, Double, String>>() {

            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1,"high temp warning");
            }

            @Override
            public Tuple3<String, Double, String> map2(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.getId(), sensorReading.getTemperature(),"normal");
            }
        });

        tempWarning.print();

        env.execute();

    }

}
