package me.iroohom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.List;

/**
 * @ClassName: BroadcastDemo
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/21 18:28
 * @Software: IntelliJ IDEA
 */
public class BroadcastDemo {

    /**
     * 将studentDS(学号,姓名)集合广播出去(广播到各个TaskManager内存中)
     * 然后使用scoreDS(学号,学科,成绩)和广播数据(学号,姓名)进行关联,得到这样格式的数据:(姓名,学科,成绩)
     */

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        studentDS：Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五"))
//        scoreDS：Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86))

        DataSource<Tuple2<Integer, String>> studentSource = env.fromCollection(Arrays.asList(Tuple2.of(1, "张三"), Tuple2.of(2, "李四"), Tuple2.of(3, "王五")));
        DataSource<Tuple3<Integer, String, Integer>> scoreSource = env.fromCollection(Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86)));

        MapOperator<Tuple3<Integer, String, Integer>, Object> result = scoreSource.map(new RichMapFunction<Tuple3<Integer, String, Integer>, Object>() {
            List<Tuple2<Integer, String>> broadcast = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取广播变量
                broadcast = getRuntimeContext().getBroadcastVariable("broadcast");
            }

            @Override
            public Object map(Tuple3<Integer, String, Integer> value) throws Exception {
                //遍历广播变量
                for (Tuple2<Integer, String> line : broadcast) {
                    //如果学生学号一样 就可以进行组合
                    if (line.f0.equals(value.f0)) {
                        return Tuple3.of(line.f1, value.f1, value.f2);
                    }
                }
                return null;
            }
        }).withBroadcastSet(studentSource, "broadcast");

        result.print();


    }
}
