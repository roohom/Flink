package me.iroohom.Stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.omg.PortableInterceptor.ObjectReferenceFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName: StreamSql
 * @Author: Roohom
 * @Function: 每隔5秒统计最近5秒的每个用户的订单总数、订单的最大金额、订单的最小金额
 * @Date: 2020/10/25 16:32
 * @Software: IntelliJ IDEA
 */
public class StreamSql {
    /**
     * 开发步骤：
     * <p>
     * 1.获取流处理执行环境
     * 2.自定义数据源
     * 3.获取表执行环境
     * 4.设置事件时间、水位线
     * 5.创建视图，sql
     * 6.表转换成流
     * 7.打印，执行
     * </p>
     * 划分窗口：tumble(eventTime, interval '5' second)
     */
    public static void main(String[] args) throws Exception {
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.自定义数据源及其加载
        DataStreamSource<Order> source = env.addSource(new StreamSourceFunction());
        //3.获取表执行环境
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);
        //4.设置事件时间，水位线
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Order> watermarkData = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.getEventTime();
            }
        });

        //5.创建视图，SQL,(每个用户的订单总数、订单的最大金额、订单的最小金额)
        tblEnv.createTemporaryView("tbl",watermarkData,"orderId,userId,money,eventTime.rowTime");

        String sql =
                "select userId, " +
                        "count(*)," +
                        "max(money)," +
                        "min(money) " +
                        "from tbl " +
                        "group by userId,tumble(eventTime,interval '5' second)";


        //6.执行SQL
        Table table = tblEnv.sqlQuery(sql);
        //表转换成流
        DataStream<Row> dataStream = tblEnv.toAppendStream(table, Row.class);
        //数据打印
        dataStream.print();
        //触发执行
        env.execute();

    }


    /**
     * 自定义数据源
     */
    private static class StreamSourceFunction extends RichSourceFunction<Order> {

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (true) {

                Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(100), System.currentTimeMillis());
                System.out.println(order);
                //随机生成一个order数据
                ctx.collect(order);
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {

        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long eventTime;
    }
}
