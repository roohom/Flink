package me.iroohom;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * @ClassName: CounterDemo
 * @Author: Roohom
 * @Function: 累加器
 * @Date: 2020/10/21 18:05
 * @Software: IntelliJ IDEA
 */
public class CounterDemo {
    /**
     * 1、初始化执行环境
     * 2、加载数据源
     * 3、数据转换
     * 注册累加器
     * 累加器添加数据
     * 获取累加器数据
     */
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.fromElements("a", "b", "c", "d");
        MapOperator<String, Object> mapOperator = source.map(new RichMapFunction<String, Object>() {
            IntCounter intCounter = new IntCounter();

            /**
             * 初始化方法，在map方法之前先执行
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                //注册累加器
                getRuntimeContext().addAccumulator("cnt", intCounter);
            }

            @Override
            public Object map(String value) throws Exception {
                intCounter.add(1);
                return value;
            }
        });
        mapOperator.writeAsText("cnt");
        //这里不能打印，打印是一个触发算子，就无法获取累加器数据

        JobExecutionResult execute = env.execute();
        Object cnt = execute.getAccumulatorResult("cnt");
        System.out.println(cnt);
    }
}
