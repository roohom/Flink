package me.iroohom;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @ClassName: SourceDemo
 * @Author: Roohom
 * @Function: Flink 批处理SourceDemo FromElements
 * @Date: 2020/10/21 10:32
 * @Software: IntelliJ IDEA
 */
public class SourceDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 1.获取批处理执行环境
         * 2.加载数据
         * 3.数据打印
         */
        //1.获取批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.fromElements("a", "b", "c","c");

        source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        }).groupBy(0)
                .sum(1)
                .print();


    }
}
