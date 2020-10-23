package me.iroohom.customizeSource;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: MySourceParallel
 * @Author: Roohom
 * @Function: 自定义数据源 并行度能大于1
 * @Date: 2020/10/22 10:32
 * @Software: IntelliJ IDEA
 */


public class MySourceParallel extends RichParallelSourceFunction<Order> {
    /**
     * 自定义数据源
     * SourceFunction:非并行数据源(并行度只能=1)
     * RichSourceFunction:多功能非并行数据源(并行度只能=1)
     * ParallelSourceFunction:并行数据源(并行度能够>=1)
     * RichParallelSourceFunction:多功能并行数据源(并行度能够>=1)--后续学习的Kafka数据源使用的就是该接口
     */
    boolean flag = true;

    @Override
    public void run(SourceContext<Order> sourceContext) throws Exception {
        Random random = new Random();
        int i = 0;
        while (flag) {
            String id = UUID.randomUUID().toString();

            sourceContext.collect(new Order(id, random.nextInt(2), random.nextInt(100), System.currentTimeMillis()));

            //线程休眠1秒
            TimeUnit.SECONDS.sleep(1);

            i++;
            if (i > 10) {
                cancel();
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
