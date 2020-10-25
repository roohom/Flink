package me.iroohom.WaterMarker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: WaterMarkerBounded
 * @Author: Roohom
 * @Function: BoundedOutOfOrdernessTimestampExtractor实现水印（水位线）
 * @Date: 2020/10/25 08:53
 * @Software: IntelliJ IDEA
 */
public class WaterMarkerBounded {
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("node1", 8090);

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

        map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Boss>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Boss element) {
                return element.getEventTime();
            }
        }).keyBy("company")
                .timeWindow(Time.seconds(3))
                .max("price");

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
