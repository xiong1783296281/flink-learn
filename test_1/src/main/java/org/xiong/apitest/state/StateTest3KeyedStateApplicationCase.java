package org.xiong.apitest.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2021/1/5
 */
public class StateTest3KeyedStateApplicationCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);
        DataStream<SensorReading> dataStream = inputStream.map(
                line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
                });

        // 定义一个有状态的map操作，统计当前分区数据个数
        DataStream<Tuple3<String,Double,Double>> mapStream = dataStream.keyBy("id").flatMap(new TempChangeWarning(10.0));
        mapStream.print();

        env.execute();

    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

        private Double threshold;

        private ValueState<Double> lastTemp;

        public TempChangeWarning(Double threshold){
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {

            Double lastTemp = this.lastTemp.value();
            if (null != lastTemp){
                double diff = Math.abs(lastTemp - sensorReading.getTemperature());
                if (diff >= threshold){
                    collector.collect(new Tuple3<>(sensorReading.getId(),lastTemp,sensorReading.getTemperature()));
                }
            }
            this.lastTemp.update(sensorReading.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }
    }

}
