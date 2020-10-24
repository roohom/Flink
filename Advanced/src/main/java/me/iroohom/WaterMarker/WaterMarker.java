package me.iroohom.WaterMarker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @ClassName: WaterMarker
 * @Author: Roohom
 * @Function: 水位线演示
 * @Date: 2020/10/23 11:36
 * @Software: IntelliJ IDEA
 */
public class WaterMarker {
    /**
     * 开发步骤：
     * 1.初始化执行环境
     * 2.设置事件时间、并行度
     * 3.加载数据源
     * 4.数据转换：新建bean对象
     * 5.设置水位线
     * 6.数据分组
     * 7.划分时间窗口
     * 8.数据聚合
     * 9.打印数据
     * 10.触发执行
     */
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //指定事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置成单线程便于测试观察
        env.setParallelism(1);

        //加载数据源
        DataStreamSource<String> source = env.socketTextStream("node1", 8090);

        //数据转换
        SingleOutputStreamOperator<Boss> map = source.map(new MapFunction<String, Boss>() {

            @Override
            public Boss map(String value) throws Exception {
                String[] split = value.split(",");

                return new Boss(
                        Long.valueOf(split[0]),
                        split[1],
                        split[2],
                        Double.valueOf(split[3])
                );
            }
        });

        //新建侧边流，收集延迟数据
        OutputTag<Boss> opt = new OutputTag<>("opt", TypeInformation.of(Boss.class));

        //设置水位线
        SingleOutputStreamOperator<Boss> result = map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Boss>() {

            //设置延迟数据,单位是毫秒，2s延迟,（最大允许的延迟时间或者乱序时间）
            final Long delayTime = 2000L;

            //设置当前时间
            Long currentTimestamp = 0L;

            /**
             * 获取水位线数据
             * 水位线是一个延迟的时间轴，就会有延迟数据
             * @return 返回新的水印（水位线）
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                //延迟时间，触发延迟计算，会延迟2s
                return new Watermark(currentTimestamp - delayTime);
            }

            /**
             * 抽取事件时间
             * @param element 单位数据(条)
             * @param previousElementTimestamp 前一条数据的时间戳
             * @return 新的时间戳，就是当前获取的新的事件时间
             */
            @Override
            public long extractTimestamp(Boss element, long previousElementTimestamp) {
                //数据源本身的时间
                Long eventTime = element.getEventTime();

                //当前时间，好处是保证时间轴一直往前不会倒退
                currentTimestamp = Math.max(eventTime, currentTimestamp);
                //返回新的时间戳，当前获取的事件时间
                return eventTime;
            }
        })
                //数据分组
                .keyBy("company")
                //每三秒触发一次窗口计算，依赖于事件时间
                .timeWindow(Time.seconds(4))
                //在延迟的时间的基础上，再延迟2秒钟
//                .allowedLateness(Time.seconds(2))
                //侧输出流，简称侧边流
//                .sideOutputLateData(opt)
                //取price最大的一组值
                .max("price");


        result.print("正常数据:");
        result.getSideOutput(opt).print("延迟数据:");

        env.execute();


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Boss {
        private Long eventTime;
        private String company;
        private String product;
        private Double price;
    }
}
