package org.xiong.apitest.enviroment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: xiong
 * @create at 2020/12/29
 */
public class ExecutionEnvironmentTest {

    public static void main(String[] args) {

        // 根据执行环境，返回执行环境，本地执行环境或远程环境
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建本地执行环境
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.createLocalEnvironment(1);

        // 创建远程执行环境
        StreamExecutionEnvironment env3 = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1",6123, "com.aicyber.wc.StreamWordCount");

    }

}
