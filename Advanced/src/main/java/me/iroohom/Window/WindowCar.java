package me.iroohom.Window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName: WindowCar
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/23 09:53
 * @Software: IntelliJ IDEA
 */
public class WindowCar {

    /**
     * 在node1上使用nc -lk 8090发送
     * 交通灯，car数量
     * 9,3
     * 9,2
     * 9,7
     * 4,9
     * 2,6
     * 1,5
     * 2,3
     * 5,7
     * 5,4
     * 测试数据，模拟每个时间段经过指定交通灯的Car数量
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("node1", 8090);

        SingleOutputStreamOperator<Car> map = source.map(new MapFunction<String, Car>() {
            @Override
            public Car map(String value) throws Exception {
                String[] split = value.split(",");

                return new Car(
                        Integer.valueOf(split[0]),
                        Integer.valueOf(split[1])
                );
            }
        });

        map.keyBy("id")
                //企业中主要使用时间窗口

                //基于时间的滚动窗口，时间窗口是三秒，每三秒钟统计一次数据量
//                .timeWindow(Time.seconds(3))

                //基于时间的滑动窗口，，每三秒统计过去6秒的数据
//                .timeWindow(Time.seconds(6), Time.seconds(3))

                //基于数量的滚动窗口，参数3指的是Key键每出现三次计算一次，与时间无关
//                .countWindow(3)

                //基于数量的滑动窗口，Key键每出现三次，计算一次过去Key键出现6次的数据量，数据会存在重复
//                .countWindow(6, 3)

                //数据终端发送时间超过10秒，会触发一次窗口计算
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))

                .sum("count")
                .print();

        env.execute();


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Car {
        private Integer id;
        private Integer count;
    }
}
