package me.iroohom;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

/**
 * @ClassName: SourceDemo2
 * @Author: Roohom
 * @Function: Flink 批处理SourceDemo FromCollection
 * @Date: 2020/10/21 10:40
 * @Software: IntelliJ IDEA
 */
public class SourceDemo2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        DataSource<Integer> source = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6));
//        source.print();
//        env.generateSequence(0,100).print();

        DataSource<String> source = env.readTextFile("datas\\city.txt");
        source.print();
    }
}
