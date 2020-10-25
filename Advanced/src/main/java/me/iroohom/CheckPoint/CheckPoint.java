package me.iroohom.CheckPoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: CheckPointDemo
 * @Author: Roohom
 * @Function: 检查点机制演示Demo
 * @Date: 2020/10/23 17:38
 * @Software: IntelliJ IDEA
 */
public class CheckPoint {
    /**
     * 开发步骤：
     * <p>
     * 1.初始化环境
     * 2.设置检查点
     * 3.自定义source
     * 4.数据转换、分组、求和
     * 5.数据打印
     * 6.触发执行
     * </>
     */
    public static void main(String[] args) throws Exception {
        //1.初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置检查点，每次检查点的触发时间
        env.enableCheckpointing(5000L);

        //设置检查点状态存储路径，在本地
        env.setStateBackend(new FsStateBackend("file:///checkpoint"));
        //获取能设置检查点间隔以及两个检查点之间延迟的检查点配置，将同一时刻能进行最大尝试的检查点数为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //超时时间，超时会自动丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //默认值Exactly-once 强一致性 有且只有一次 保证数据只消费一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //当任务取消的时候，保留检查点，企业中主要使用的方式，需要手动删除无效的检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**
         *  设置重启策略,每个三秒重启一起，总共重启三次，如果不设置重启策略，将会无限重启
         *  1.固定延迟重启策略
         *  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));
         *  2.失败率重启策略
         *  env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(60), Time.seconds(5)));
         *  3.不重启策略
         *  env.setRestartStrategy(RestartStrategies.noRestart());
         */


        //添加自定义数据源
        env.addSource(new CheckPointSource())
                .keyBy(0)
                .sum(1)
                .print();
        env.execute();


    }

    /**
     * 自定义检查点数据源
     */
    private static class CheckPointSource extends RichSourceFunction<Tuple2<String, Integer>> {

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            int count = 0;
            while (true) {
                count++;
                ctx.collect(Tuple2.of("aa", count));
                TimeUnit.SECONDS.sleep(1);
                //模拟产生异常错误
                if (count > 10) {
                    throw new RuntimeException();
                }
            }
        }

        @Override
        public void cancel() {

        }
    }
}
