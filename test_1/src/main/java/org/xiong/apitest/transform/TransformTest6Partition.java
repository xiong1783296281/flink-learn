package org.xiong.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2020/12/30
 */
public class TransformTest6Partition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> input = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");
        DataStream<SensorReading> dataStream = input.map(
                line->{
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
                }
        );
        input.print("input");

        // 1.shuffle
        DataStream<String> shuffleStream = input.shuffle();

//        shuffleStream.print("shuffle");

        // 2.keyby
//        dataStream.keyBy("id").print("keyBy");

        // 3.global
        dataStream.global().print("global");

        env.execute();


    }

}
