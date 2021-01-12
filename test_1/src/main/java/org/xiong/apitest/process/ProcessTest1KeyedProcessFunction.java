package org.xiong.apitest.process;

import org.apache.flink.api.java.tuple.Tuple;
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
public class ProcessTest1KeyedProcessFunction {

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
        dataStream.keyBy("id")
                .process(new MyProcess())
                .print();

        env.execute();

    }

    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer>{

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<Integer> collector) throws Exception {
            collector.collect(sensorReading.getId().length());

            // context
            context.timestamp(); //当前的时间戳
            context.getCurrentKey(); // 获取当前的key
//            context.output(new OutputTag<>("late"));
            context.timerService().currentProcessingTime(); // 当前处理时间
            context.timerService().currentWatermark();  // 当前数据时间
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+1000L); // 绝对时间
            context.timerService().registerEventTimeTimer((sensorReading.getTimestamp()+10)*1000L);
            context.timerService().deleteProcessingTimeTimer(10000L);
//            context.timerService().deleteEventTimeTimer();

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println("当前时间戳 " +timestamp);
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();

        }
    }

}
