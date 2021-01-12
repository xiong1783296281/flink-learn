package org.xiong.apitest.process;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.xiong.apitest.beans.SensorReading;

/**
 * @author: xiong
 * @create at 2021/1/7
 */
public class ProcessTest3SideOutCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 9999);
        DataStream<SensorReading> dataStream = inputStream.map(
                line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
                });

        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("lowTemp") {
        };

        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new MyProcess(lowTempTag));
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");


        env.execute();

    }

    public static class MyProcess extends ProcessFunction<SensorReading,SensorReading>{

        private final OutputTag<SensorReading> lowTempTag;

        public MyProcess(OutputTag<SensorReading> lowTempTag){
            this.lowTempTag = lowTempTag;
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {

            if (sensorReading.getTemperature() > 30){
                collector.collect(sensorReading);
            }else{
                context.output(lowTempTag,sensorReading);
            }

        }
    }

}
