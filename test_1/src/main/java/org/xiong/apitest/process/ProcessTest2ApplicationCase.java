package org.xiong.apitest.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2021/1/6
 */
public class ProcessTest2ApplicationCase {

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
        DataStream<String> mapStream = dataStream.keyBy("id")
                .process(new TempChangeWarning(10));
        mapStream.print();

        env.execute();

    }

    public static class TempChangeWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private Integer interval;

        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        public TempChangeWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE));
            timerTsState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timeTs = timerTsState.value();

            if (sensorReading.getTemperature() > lastTemp && timeTs == null){
                long ts = context.timerService().currentProcessingTime() + interval * 1000L;
                context.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            else if ( sensorReading.getTemperature() < lastTemp && timeTs != null){
                context.timerService().deleteProcessingTimeTimer(timeTs);
                timerTsState.clear();
            }

            lastTempState.update(sensorReading.getTemperature());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器"+ctx.getCurrentKey().getField(0)+"温度值持续"+interval+"s上升");
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
            timerTsState.clear();
        }
    }

}
