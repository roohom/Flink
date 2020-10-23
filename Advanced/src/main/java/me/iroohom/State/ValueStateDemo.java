package me.iroohom.State;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.state.ValueState;

/**
 * @ClassName: ValueState
 * @Author: Roohom
 * @Function: ValueStateDemo 分组求和
 * @Date: 2020/10/23 16:19
 * @Software: IntelliJ IDEA
 */
public class ValueStateDemo {
    /**
     * 需求：分组求和
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.加载数据源
     * 3.数据分组
     * 4.数据转换，定义ValueState,保存中间结果
     * 5.数据打印
     * 6.触发执行
     */
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //加载数据源
        DataStreamSource<Tuple2<String, Long>> source = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)

        );

        //数据分组，根据地名分组
        source.keyBy(0)
                //数据转换，自定义valueState
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String,Long>>() {

                    ValueState<Long> valueState = null;

                    /**
                     * 初始化方法，只执行一次，获取ValueState
                     * @param parameters
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //通过上下文获取valueState
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("vs", Long.class));
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                        //如果状态值为空就设置为0，否则设置为本身
                        long count = valueState.value() == null ? 0L : valueState.value();
                        count += value.f1;
                        //更新状态
                        valueState.update(count);
                        return Tuple2.of(value.f0, count);
                    }
                }).print();
        env.execute();
    }
}
