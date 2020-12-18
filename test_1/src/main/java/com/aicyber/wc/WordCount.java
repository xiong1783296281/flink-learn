package com.aicyber.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 批处理
 *
 * @author: xiong
 * @create at 2020/12/14
 */
public class WordCount {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filepath = "/home/xiong/IdeaProjects/spark-learn/test_1/src/main/resources/test.txt";
        DataSet<String> input = env.readTextFile(filepath);
        DataSet<Tuple2<String,Integer>> res = input.flatMap(new FlatMapTest())
                .groupBy(0)
                .sum(1);
        res.print();
    }

    public static class FlatMapTest implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words){
                out.collect(new Tuple2<>(word,1));
            }

        }
    }

}
