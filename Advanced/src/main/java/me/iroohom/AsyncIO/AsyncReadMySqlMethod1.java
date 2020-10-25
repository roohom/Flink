package me.iroohom.AsyncIO;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: AsyncReadMySql
 * @Author: Roohom
 * @Function: 使用vertx异步读取MySQL
 * @Date: 2020/10/25 19:55
 * @Software: IntelliJ IDEA
 */
public class AsyncReadMySqlMethod1 {

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

        //方式一:JDBC方式
        AsyncDataStream.unorderedWait(source, new AsyncIoMethod1(), 60, TimeUnit.SECONDS).print();

        env.execute();

    }

    /**
     * 自定义异步数据源
     */
    private static class AsyncIoSource extends RichSourceFunction<CategoryInfo> {
        @Override
        public void run(SourceContext ctx) throws Exception {
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

    private static class AsyncIoMethod1 extends RichAsyncFunction<CategoryInfo, CategoryInfo> {
        JDBCClient jdbcClient = null;

        /**
         * 初始化连接池
         *
         * @param parameters pass
         * @throws Exception pass
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            JsonObject jsonObject = new JsonObject();
            jsonObject
                    .put("driver_class", "com.mysql.cj.jdbc.Driver")
                    .put("url", "jdbc:mysql://localhost:3306/roohom?autoReconnect=true&useSSL=false&characterEncoding=" +
                            "utf-8&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai")
                    .put("user", "root")
                    .put("password", "123456");

            VertxOptions options = new VertxOptions();
            //设置循环线程数
            options.setEventLoopPoolSize(10);
            //设置最大线程数
            options.setWorkerPoolSize(10);

            Vertx vertx = Vertx.vertx(options);
            jdbcClient = JDBCClient.create(vertx, jsonObject);
        }

        /**
         * 异步唤醒程序
         *
         * @param input        输入参数CategoryInfo
         * @param resultFuture pass
         * @throws Exception pass
         */
        @Override
        public void asyncInvoke(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
            jdbcClient.getConnection(new Handler<AsyncResult<SQLConnection>>() {
                @Override
                public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                    sqlConnectionAsyncResult.result().query("select * from t_category where id = " + input.getId(), new Handler<AsyncResult<ResultSet>>() {
                        @Override
                        public void handle(AsyncResult<ResultSet> resultSetAsyncResult) {
                            //获取异步返回结果
                            ResultSet result = resultSetAsyncResult.result();
                            List<JsonObject> rows = result.getRows();
                            for (JsonObject row : rows) {
                                //取出name
                                String name = row.getString("name");
                                //收集数据
                                resultFuture.complete(Collections.singleton(new CategoryInfo(input.getId(), name)));
                            }
                        }
                    });
                }
            });
        }
    }
}
