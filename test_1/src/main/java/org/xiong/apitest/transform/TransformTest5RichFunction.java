package org.xiong.apitest.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2020/12/30
 */
public class TransformTest5RichFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");

        DataStream<SensorReading> dataStream = streamSource.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
                }
        );

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMap());
        resultStream.print();

        env.execute();

    }

    public static class MyMap0 implements MapFunction<SensorReading,Tuple2<String,Integer>>{
        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),sensorReading.getId().length());
        }
    }

    public static class MyMap extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
//            getRuntimeContext().getState();
            return new Tuple2<>(sensorReading.getId(),sensorReading.getId().length());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作 一般是定义工作状态或 创建数据库连接，es连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            // 一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }

}
