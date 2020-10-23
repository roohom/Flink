package me.iroohom.Sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;

/**
 * @ClassName: SinkRedis
 * @Author: Roohom
 * @Function: 将单词统计求和结果数据，通过自定义Sink保存到Redis
 * @Date: 2020/10/22 19:42
 * @Software: IntelliJ IDEA
 */
public class SinkRedis {
    /**
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.加载socket数据，开启nc
     * 3.数据转换/求和（flatMap/map/keyBy/sum）
     * 4.自定义sink对象:RedisSink
     * （1）定义redis连接池（集群）
     * （2）实现redisMapper
     * 5.触发执行
     */
    public static void main(String[] args) throws Exception {
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.加载socket数据，开启nc
        DataStreamSource<String> source = env.socketTextStream("node2", 8090);

        //3.数据转换/求和（flatMap/map/keyBy/sum）
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split("\\w+");
                for (String s : split) {
                    out.collect(s);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(0)
                .sum(1);

        //4.自定义sink对象:RedisSink
        //（1）定义redis连接池（集群）
        HashSet<InetSocketAddress> redisNodesSet = new HashSet<>();
        //一定要使用InetAddress.getByName("node1")，不能直接使用“node1”，大坑，大师，我悟了
        redisNodesSet.add(new InetSocketAddress(InetAddress.getByName("node1"), 7001));
        redisNodesSet.add(new InetSocketAddress(InetAddress.getByName("node1"), 7002));
        redisNodesSet.add(new InetSocketAddress(InetAddress.getByName("node1"), 7003));

        //连接池
        FlinkJedisClusterConfig clusterConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(redisNodesSet)
                .setMaxTotal(5)
                .setMaxIdle(3)
                .setMinIdle(1)
                .build();


        //（2）实现redisMapper
        sum.addSink(new RedisSink<>(clusterConfig, new RedisMapper<Tuple2<String, Integer>>() {

            /**
             * Returns descriptor which defines data type.
             * @return Returns descriptor which defines data type.
             */
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "2");
            }

            /**
             * Extracts key from data. 具体写入数据的key
             * @param data data
             * @return key of data
             */
            @Override
            public String getKeyFromData(Tuple2<String, Integer> data) {
                return data.f0;
            }

            /**
             * Extracts value from data.具体写入求和的值
             * @param data data
             * @return value of data
             */
            @Override
            public String getValueFromData(Tuple2<String, Integer> data) {
                return data.f1.toString();
            }
        }));

        //触发执行
        env.execute();


    }
}
