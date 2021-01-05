package org.xiong.apitest.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xiong.apitest.beans.SensorReading;
import org.xiong.apitest.source.SourceTest4CustomFunction;

/**
 * @author: xiong
 * @create at 2021/1/4
 */
public class WindowTest2CountWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceTest4CustomFunction.MyCustomSource());

        DataStream<Tuple2<String, Double>> resultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        resultStream.print();

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple3<String,Double,Integer>, Tuple2<String, Double>>{


        @Override
        public Tuple3<String, Double, Integer> createAccumulator() {
            return new Tuple3<>("",0.0,0);
        }

        @Override
        public Tuple3<String, Double, Integer> add(SensorReading sensorReading, Tuple3<String, Double, Integer> acc) {
            return new Tuple3<>(sensorReading.getId(),acc.f1+sensorReading.getTemperature(),acc.f2+1);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> acc) {
            return new Tuple2<>(acc.f0, acc.f1/acc.f2);
        }

        @Override
        public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc, Tuple3<String, Double, Integer> acc1) {
            return new Tuple3<>(acc.f0, acc.f1+acc1.f1,acc.f2+acc1.f2);
        }
    }

}
