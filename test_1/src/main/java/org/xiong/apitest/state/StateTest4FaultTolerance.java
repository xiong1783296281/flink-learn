package org.xiong.apitest.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author: xiong
 * @create at 2021/1/7
 */
public class StateTest4FaultTolerance {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.状态后端配置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new RocksDBStateBackend(""));

        // 2.检查点配置
        env.enableCheckpointing(1000L); // 隔1s保存checkpoint

        // 高级选项
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 当前远程保存的磁盘存满了 或者 网络出了问题，防止同步checkpoint超时
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        // 设置同时有两个checkpoint的 barrier 进来
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 两个checkpoint间隔的时间，上一个checkpoint保存结束后(950ms，下个checkpoint得等待50ms)100ms内不允许任何checkpoint进行保存操作 这之间是数据处理的时间
        // 因此会覆盖MaxConcurrentCheckpoints的配置
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);
        // 默认为false
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 允许checkpoint保存失败多少次
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);

        // 3.指定重启策略
        // 固定延迟重启 最大重启次数以及重启间隔时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        // 失败率重启 10分钟之内重启未成功，间隔一分钟继续重启，最大尝试次数:3次
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));

        env.execute();

    }

}
