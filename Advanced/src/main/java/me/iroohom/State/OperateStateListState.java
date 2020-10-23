package me.iroohom.State;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @ClassName: OperateState
 * @Author: Roohom
 * @Function: OperateState 模拟kafka偏移量存储
 * @Date: 2020/10/23 16:40
 * @Software: IntelliJ IDEA
 */
public class OperateStateListState {
    /**
     * 需求：模拟kafka偏移量存储
     * 步骤：
     * 1.获取执行环境
     * 2.设置检查点机制：路径，重启策略
     * 3.自定义数据源
     *  (1)需要继承数据源和CheckpointedFunction
     *  (2)设置listState,通过上下文对象context获取
     *  (3)数据处理，保留offset
     * (4)制作快照
     * 4.数据打印
     * 5.触发执行
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        env.setStateBackend(new FsStateBackend("file:///checkpoint"));

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //重启策略，当出现异常停机的时候，会自动拉起
        //固定延迟重启策略，共重启3次，每次重启时间间隔3秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(3)));

        //自定义数据源
        env.addSource(new KafkaSource()).print();

        //触发执行
        env.execute();

    }

    /**
     * 自定义模拟kafka数据源类
     * （1）需要继承数据源和CheckpointedFunction(制作检查点)
     */
    private static class KafkaSource extends RichSourceFunction<Long> implements CheckpointedFunction {
        ListState<Long> listState = null;
        Long offset = 0L;
        Boolean flag = true;

        /**
         * 制作快照
         *
         * @param context 绘制算子快照的上下文
         * @throws Exception 可能的异常
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(offset);
        }

        /**
         * 初始化状态
         *
         * @param context 绘制算子快照的上下文
         * @throws Exception 可能的异常
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Long>("ls", Long.class));
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            for (Long aLong : listState.get()) {
                offset = aLong;
            }
            while (flag) {
                offset++;
                ctx.collect(offset);
                TimeUnit.SECONDS.sleep(1);
                if (offset > 9) {
                    throw new RuntimeException("手动设置的运行时异常！");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
