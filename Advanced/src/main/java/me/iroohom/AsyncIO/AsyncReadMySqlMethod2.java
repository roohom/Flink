package me.iroohom.AsyncIO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: AsyncReadMySqlMethod2
 * @Author: Roohom
 * @Function: 使用线程池异步读取MySQL
 * @Date: 2020/10/25 20:36
 * @Software: IntelliJ IDEA
 */
public class AsyncReadMySqlMethod2 {
    /**
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.自定义数据源
     * 3.异步IO<AsyncDataStream>
     * 方式一：JDBCClient
     * 方式二：线程池
     * 4.数据打印
     * 5.触发执行
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<CategoryInfo> source = env.addSource(new AsyncIoSource());

        //方式二：线程池
        AsyncDataStream.unorderedWait(source, new AsyncMethod2(), 60, TimeUnit.SECONDS).print();

        env.execute();

    }

    /**
     * 自定义数据源
     */
    private static class AsyncIoSource extends RichSourceFunction<CategoryInfo> {

        /**
         * 收集数据，将name设置为null
         *
         * @param ctx 上下文
         * @throws Exception pass
         */
        @Override
        public void run(SourceContext<CategoryInfo> ctx) throws Exception {
            Integer[] ids = {1, 2, 3, 4, 5};
            for (Integer id : ids) {
                ctx.collect(new CategoryInfo(id, null));
            }
        }

        @Override
        public void cancel() {

        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryInfo {
        private Integer id;
        private String name;
    }

    private static class AsyncMethod2 extends RichAsyncFunction<CategoryInfo, CategoryInfo> {
        ThreadPoolExecutor threadPoolExecutor = null;

        /**
         * 初始化多线程连接池,得到线程执行器
         *
         * @param parameters pass
         * @throws Exception pass
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            /**
             * 先执行核心线程数
             * 阻塞队列放置待处理的线程，当核心线程资源不够的时候，会执行阻塞队列里的线程资源
             * 当阻塞队列的线程资源也不够处理并发任务的时候，会开启最大线程数，知道用完
             * corePoolSize
             * maximumPoolSize
             * keepAliveTime
             * TimeUnit
             * ArrayBlockQueue
             */
            threadPoolExecutor = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(5));

        }

        @Override
        public void asyncInvoke(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
            threadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    CategoryInfo query = DbUtil.query(input);
                    resultFuture.complete(Collections.singleton(query));
                }
            });
        }
    }
}
