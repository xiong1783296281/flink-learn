package org.xiong.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.xiong.apitest.beans.SensorReading;

import java.util.HashMap;
import java.util.Random;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class SourceTest4CustomFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> streamDataSource = env.addSource(new MyCustomSource());

        streamDataSource.print();

        env.execute();

    }

    public static class MyCustomSource implements SourceFunction<SensorReading>{

        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            Random random = new Random();

            HashMap<String,Double> sourceTempMap = new HashMap<>();
            for(int i=0;i<10;i++){
                sourceTempMap.put("sensor_"+(i+1), 60+random.nextGaussian()*20);
            }

            while (running){
                for(String key:sourceTempMap.keySet()) {
                    Double newtemp = sourceTempMap.get(key) + random.nextGaussian();
                    sourceTempMap.put(key,newtemp);
                    sourceContext.collect(new SensorReading(key,System.currentTimeMillis(),newtemp));
                }

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
