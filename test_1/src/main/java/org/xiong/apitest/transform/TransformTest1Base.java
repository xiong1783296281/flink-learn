package org.xiong.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class TransformTest1Base {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");

        DataStream<Integer> map = streamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        DataStream<String> flatMapStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field:fields) {
                    collector.collect(field);
                }
            }
        });

        DataStream<String> filterStream = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                String[] fields = s.split(",");
                if ("sensor2".equals(fields[0])){
                    return true;
                }
                return false;
            }
        });

        map.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        env.execute();

    }

}
