package me.iroohom.BroacastStream;

//import org.apache.flink.api.common.state.BroadcastState;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.tuple.Tuple6;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//import org.apache.flink.util.Collector;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;


import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * @ClassName: BroadcastStreamOut
 * @Author: Roohom
 * @Function: 广播流的使用
 * @Date: 2020/10/26 20:12
 * @Software: IntelliJ IDEA
 */
public class BroadcastStreamOut {

    /**
     * 需求：将实时流中的数据，与mysql中的配置变量数据，根据用户id进行组合，形成一个新的数据进行输出
     * 知识点：用到了广播流，用到mapState
     * <p>
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.自定义数据源
     * 3.获取mysql数据源: Map<String, Tuple2<String, Integer>>
     * 4.定义mapState广播变量，获取mysql广播流
     * 5.事件流和广播流连接
     * 6.业务处理BroadcastProcessFunction，补全数据
     * 7.数据打印
     * 8.触发执行
     * <p/>
     */
    public static void main(String[] args) throws Exception {
        //初始化流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //加载数据源,模拟的实时数据流
        DataStreamSource<Tuple4<String, String, String, Integer>> source = env.addSource(new BroadcastSource());

        //添加MySQL数据源
        DataStreamSource<Map<String, Tuple2<String, Integer>>> mysqlSource = env.addSource(new MySqlSource());


        //自定义mapState广播变量，获取MySQL广播流
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> ms = new MapStateDescriptor<>("ms", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
        //广播流
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastSource = mysqlSource.broadcast(ms);

        //事件流和广播流链接
        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>> connectedStream = source.connect(broadcastSource);

        //业务处理BroadcastProcessFunction，补全数据
        connectedStream.process(new BroadcastProcessFunction<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>, Object>() {

            /**
             * 事件流一侧的数据对广播流是只读的
             * @param value 输入的流元素
             * @param ctx 上下文
             * @param out 装入结果元素的收集器（迭代器）
             * @throws Exception 异常pass
             */
            @Override
            public void processElement(Tuple4<String, String, String, Integer> value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                //获取广播流数据
                ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(ms);
                Map<String, Tuple2<String, Integer>> map = broadcastState.get(null);
                if (map != null) {
                    Tuple2<String, Integer> tmpMap = map.get(value.f0);
                    //如果不为null，说明此key键即存在于mysql，也存在于事件流中，所以可以进行数据补全
                    if (tmpMap != null) {
                        out.collect(Tuple6.of(value.f0, value.f1, value.f2, value.f3, tmpMap.f0, tmpMap.f1));
                    }
                }

            }

            /**
             * 广播流一侧的数据，可以对mapState进行操作
             * @param value 输入的流元素
             * @param ctx 上下文
             * @param out 装入结果元素的收集器（迭代器）
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Map<String, Tuple2<String, Integer>> value, Context ctx, Collector<Object> out) throws Exception {
                //获取广播流
                BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(ms);
                broadcastState.put(null, value);
            }
        }).print();

        env.execute();

    }


    /**
     * 自定义数据源
     * Tuple4.of("user_3", "2019-08-17 12:19:47", "browse", 1)
     * Tuple4.of("user_2", "2019-08-17 12:19:48", "click" , 1)
     */
    private static class BroadcastSource extends RichSourceFunction<Tuple4<String, String, String, Integer>> {

        /**
         * 实际运行函数，实际的处理逻辑
         *
         * @param ctx 上下文
         * @throws Exception 异常
         */
        @Override
        public void run(SourceContext<Tuple4<String, String, String, Integer>> ctx) throws Exception {
            while (true) {
                ctx.collect(Tuple4.of("user_3", "2019-08-17 12:19:47", "browse", 1));
                ctx.collect(Tuple4.of("user_2", "2019-08-17 12:19:48", "click", 1));
                TimeUnit.SECONDS.sleep(5);
            }
        }

        @Override
        public void cancel() {

        }
    }

    /**
     * 自定义MySQL数据源
     */
    private static class MySqlSource extends RichSourceFunction<Map<String, Tuple2<String, Integer>>> {
        Connection conn = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;

        /**
         * 初始化方法，初始化与MySQL的链接
         *
         * @param parameters 参数pass
         * @throws Exception 异常pass
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            //注册驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            //建立连接
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/roohom?autoReconnect=true&useSSL=false&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai", "root", "123456");
            preparedStatement = conn.prepareStatement("select * from user_info");
            resultSet = preparedStatement.executeQuery();

        }

        /**
         * 实际的处理逻辑
         *
         * @param ctx 上下文
         * @throws Exception 异常pass
         */
        @Override
        public void run(SourceContext<Map<String, Tuple2<String, Integer>>> ctx) throws Exception {
            HashMap<String, Tuple2<String, Integer>> map = new HashMap<>();
            while (resultSet.next()) {
                String userId = resultSet.getString(1);
                String userName = resultSet.getString(2);
                int userAge = resultSet.getInt(3);
                map.put(userId, Tuple2.of(userName, userAge));
            }
            ctx.collect(map);
        }


        @Override
        public void close() throws Exception {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

        @Override
        public void cancel() {
        }
    }
}
