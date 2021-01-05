package org.xiong.apitest.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.xiong.apitest.beans.SensorReading;
import org.xiong.apitest.source.SourceTest4CustomFunction;

import java.util.Properties;

/**
 * @author: xiong
 * @create at 2021/1/4
 */
public class WindowTest1TimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> inputStream = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");
//        DataStream<SensorReading> dataStream = inputStream.map(
//                line->{
//                    String[] fields = line.split(",");
//                    return new SensorReading(fields[0],Long.valueOf(fields[1]),Double.valueOf(fields[2]));
//                }
//        );

        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceTest4CustomFunction.MyCustomSource());

        //开窗测试
        DataStream<Integer> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .timeWindow(Time.seconds(15),Time.seconds(5))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });

        resultStream.print();

        OutputTag<SensorReading> outputTag = new OutputTag<>("late");


        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .trigger()
//                .evictor()
                // 允许数据迟到一分钟再进行计算
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");


        env.execute();

    }

}
