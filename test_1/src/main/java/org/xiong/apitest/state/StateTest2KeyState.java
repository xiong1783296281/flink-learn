package org.xiong.apitest.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2021/1/5
 */
public class StateTest2KeyState {

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
        DataStream<Integer> mapStream = dataStream.keyBy("id").map(new MyKeyCountMapper());
        mapStream.print();

        env.execute();

    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer>{

        private ValueState<Integer> keyCount;

        private ListState<String> myListState;
        private MapState<String,Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCount = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));

            myListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<String>("my-list", String.class));

            myMapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<String, Double>("my-map",String.class, Double.class));
            myReducingState = getRuntimeContext()
                    .getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce", new ReduceFunction<SensorReading>() {
                        @Override
                        public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                            return new SensorReading(sensorReading.getId(),t1.getTimestamp(),sensorReading.getTemperature()+t1.getTemperature());
                        }
                    }, SensorReading.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCount.value();
            count++;
            keyCount.update(count);

            for (String str : myListState.get()){
                System.out.println(str);
            }
            myListState.add(sensorReading.getId());

            myMapState.put(sensorReading.getId(),sensorReading.getTemperature());
            myReducingState.add(sensorReading);
            myReducingState.clear();

            return count;
        }
    }

}
