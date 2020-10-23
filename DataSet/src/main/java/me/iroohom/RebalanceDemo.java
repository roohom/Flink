package me.iroohom;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * @ClassName: RebalanceDemo
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/21 17:27
 * @Software: IntelliJ IDEA
 */
public class RebalanceDemo {
    /**
     * 需求:使用再平衡算子，均匀地打印数据，数据结构:(线程ID，数据)
     * 开发步骤:
     * 1.获取初始化执行环境
     * 2.加载数据源
     * 3.数据转换，RichMapFunction
     * 4.数据打印
     */
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        FilterOperator<Long> filterOperator = env.generateSequence(0, 100)
                .filter(new FilterFunction<Long>() {
                    @Override
                    public boolean filter(Long value) throws Exception {
                        return value < 50;
                    }
                });
        
        filterOperator
                .rebalance()
                .map(new RichMapFunction<Long, Tuple2<Integer,Long>>() {
            int id = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                //通过上下文对象获取线程ID
                id = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public Tuple2<Integer, Long> map(Long value) throws Exception {
                return Tuple2.of(id,value);
            }
        })
        .print();
    }
}
