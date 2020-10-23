package me.iroohom.Transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName: UnionConnectDemo
 * @Author: Roohom
 * @Function: Transformation Union Connect 演示
 * @Date: 2020/10/22 18:05
 * @Software: IntelliJ IDEA
 */
public class UnionConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source1 = env.fromElements("a", "b", "c");
        DataStreamSource<String> source2 = env.fromElements("a1", "b2", "c3");
        DataStreamSource<Integer> source3 = env.fromElements(1, 2, 3);

        //多条数据流类型必须一致
        //source1.union(source2).print();

        source1.connect(source3).map(new CoMapFunction<String, Integer, Object>() {

            /**
             * This method is called for each element in the first of the connected streams.
             */
            @Override
            public Object map1(String s) throws Exception {
                return s;
            }

            @Override
            public Object map2(Integer integer) throws Exception {
                return integer+"==================";
            }
        }).print();

        env.execute();
    }
}
