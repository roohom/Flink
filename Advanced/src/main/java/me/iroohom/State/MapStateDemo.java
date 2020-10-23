package me.iroohom.State;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: MapState
 * @Author: Roohom
 * @Function: MapState 分组求和
 * @Date: 2020/10/23 16:31
 * @Software: IntelliJ IDEA
 */
public class MapStateDemo {
    /**
     * 需求：使用MapState保存中间结果对下面数据进行分组求和
     * <p>
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.加载数据源
     * 3.数据分组
     * 4.数据转换，定义MapState,保存中间结果
     * 5.数据打印
     * 6.触发执行
     * <p/>
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> source = env.fromElements(
                Tuple2.of("java", 1),
                Tuple2.of("python", 3),
                Tuple2.of("java", 2),
                Tuple2.of("scala", 2),
                Tuple2.of("python", 1),
                Tuple2.of("java", 1),
                Tuple2.of("scala", 2)

        );

        source.keyBy(0)
                //4.数据转换，定义MapState,保存中间结果
                .map(new RichMapFunction<Tuple2<String, Integer>, Object>() {
                    MapState<String, Integer> mapState = null;

                    /**
                     * 初始化方法，在真正的处理函数执行前执行，用作一次性初始设置
                     * @param parameters
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //根据上下文对象获取mapState
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("ms", String.class, Integer.class));
                    }

                    @Override
                    public Object map(Tuple2<String, Integer> value) throws Exception {
                        //获取中间状态数据,如果为空就设置为0，不为空就设置为本身
                        int count = mapState.get(value.f0) == null ? 0 : mapState.get(value.f0);
                        count += value.f1;

                        //缓存中间计算结果，到mapState
                        mapState.put(value.f0, count);
                        return Tuple2.of(value.f0, count);
                    }
                }).print();
        env.execute();
    }
}
