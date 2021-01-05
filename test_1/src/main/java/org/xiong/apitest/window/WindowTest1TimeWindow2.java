package org.xiong.apitest.window;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.xiong.apitest.beans.SensorReading;
import org.xiong.apitest.source.SourceTest4CustomFunction;

/**
 * @author: xiong
 * @create at 2021/1/4
 */
public class WindowTest1TimeWindow2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream = env.addSource(new SourceTest4CustomFunction.MyCustomSource());

        //开窗测试
        DataStream<Tuple3<String, Long, Integer>> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .timeWindow(Time.seconds(15),Time.seconds(5))
                // 全窗口函数，收集齐所有数据再开始计算
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String,Long,Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = timeWindow.getEnd();
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3(id,windowEnd,count));
                    }
                });

        resultStream.print();

        env.execute();

    }

}
