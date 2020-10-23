package me.iroohom.customizeSource;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StreamSourceDemo
 * @Author: Roohom
 * @Function: 自定义流SourceDemo
 * @Date: 2020/10/22 10:07
 * @Software: IntelliJ IDEA
 */
public class StreamSourceDemo {

    /**
     * 开发步骤
     * 1.获取流处理执行环境
     * 2.自定义数据源
     * 3.数据读取
     * 4.触发执行
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new MySourceParallel()).setParallelism(2).print();

        //触发执行
        env.execute();
    }
}
