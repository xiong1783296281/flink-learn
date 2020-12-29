package org.xiong.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class SourceTest2Text {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("/home/xiong/IdeaProjects/flink-learn/test_1/src/main/resources/testTxt.txt");

        dataStreamSource.print();

        env.execute();

    }
}
