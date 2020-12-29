package org.xiong.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.xiong.apitest.beans.SensorReading;

import java.util.Arrays;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class SourceTest1Collection {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> dataStreamSource = env.fromCollection(Arrays.asList(new SensorReading("sensor1", 12312441L, 17.5),
                new SensorReading("sensor2", 123124415L, 17.5),
                new SensorReading("sensor3", 123124413L, 19.5),
                new SensorReading("sensor4", 123124414L, 16.5),
                new SensorReading("sensor5", 123452354L, 18.5)));

        DataStreamSource<Integer> integerStreamSource = env.fromElements(1, 234, 56, 67, 23);

        dataStreamSource.print("data");
        integerStreamSource.print("int");

        env.execute("source_test_collection1");

    }


}
