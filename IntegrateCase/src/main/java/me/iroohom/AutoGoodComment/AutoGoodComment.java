package me.iroohom.AutoGoodComment;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: AutoGoodComment
 * @Author: Roohom
 * @Function: 定时器的使用，实现自动好评
 * @Date: 2020/10/26 11:02
 * @Software: IntelliJ IDEA
 */
public class AutoGoodComment {
    /**
     * 开发步骤：
     * 1.初始化流处理执行环境
     * 2.加载数据源
     * 3.分组
     * 4.process数据转换
     * 5.初始化MapState
     * 6.注册定时器
     * 7.判断是否评价过
     * 8.触发执行
     */
    public static void main(String[] args) throws Exception {

        //获取初始化流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //加载数据源
        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new AutoSource());

        //Transformation 数据分组
        source.keyBy(0)
                .process(new ProcessFunctionAuto());

        env.execute();

    }

    /**
     * 自定义数据源
     */
    private static class AutoSource extends RichSourceFunction<Tuple2<String, Long>> {

        Boolean flag = true;

        /**
         * 执行方法，实际处理的逻辑函数
         *
         * @param ctx 上下文
         * @throws Exception 异常
         */
        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            int i = 0;
            while (flag) {
                ctx.collect(Tuple2.of(UUID.randomUUID().toString(), System.currentTimeMillis()));
                TimeUnit.SECONDS.sleep(1);
                i++;
                if (i == 10) {
                    cancel();
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    private static class ProcessFunctionAuto extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, Object> {
        MapState<String, Long> mapState = null;

        /**
         * 初始化方法
         *
         * @param parameters 参数pass
         * @throws Exception 异常pass
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("ms", String.class, Long.class));
        }

        /**
         * 处理每一个元素
         *
         * @param value 输入参数
         * @param ctx   上下文
         * @param out   输出参数
         * @throws Exception 异常pass
         */
        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            mapState.put(value.f0, value.f1);
            ctx.timerService().registerProcessingTimeTimer(5000);
        }


        /**
         * 注册定时器
         *
         * @param timestamp 时间戳
         * @param ctx       上下文
         * @param out       输出参数
         * @throws Exception 异常pass
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            Iterable<Map.Entry<String, Long>> entries = mapState.entries();
            for (Map.Entry<String, Long> entry : entries) {
                String key = entry.getKey();
                if (key.hashCode() % 2 == 0) {
                    System.out.println(entry.getValue() + "\t 已好评！");
                } else {
                    System.out.println(entry.getValue() + "\t 未好评！");
                }
            }
        }
    }
}
