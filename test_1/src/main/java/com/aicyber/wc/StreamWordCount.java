package com.aicyber.wc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

/**
 * 流处理
 *
 * @author: xiong
 * @create at 2020/12/15
 */
@Slf4j
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行
        env.setParallelism(1);

//        String filepath = "/opt/test.txt";
//        DataStream<String> inputStream = env.readTextFile(filepath);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        log.info("host: "+host+" port: "+port);

        DataStream<String> inputStream = env.socketTextStream(host,port);

        DataStream<WordInfo> resultStream = inputStream.flatMap(new WordInfoMapper())
                .keyBy("word")
                .sum("count").setParallelism(2);

//        DataStream<Tuple2<String, Integer>> resultStream = inputStream.flatMap(new WordCount.FlatMapTest())
//                .keyBy(0)
//                .sum(1);

        resultStream.print().setParallelism(1);

        env.execute();
    }

}
