package me.iroohom.Sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName: SinkKafkaDemo
 * @Author: Roohom
 * @Function: 将Socket流中的数据通过自定义Sink保存到Kafka TODO:本程序同FlinkProducerToKafkaSink
 * @Date: 2020/10/22 18:17
 * @Software: IntelliJ IDEA
 */
public class SinkKafkaDemo {
    /**
     * 需求：将 Socket流中的的数据通过自定义Sink保存到Kafka
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.加载数据源，加载nc
     * 3.配置kafka生产参数
     * 4.获取kafka生产者对象
     * 5.数据写入kafka
     * 6.触发执行
     */
    public static void main(String[] args) throws Exception {
        //获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //加载数据源
        DataStreamSource<String> source = env.socketTextStream("node2", 8090);

        //配置kafka生产者参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");

        //获取Kafka生产者对象
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("test2", new SimpleStringSchema(), properties);
        //数据写入Kafka
        source.addSink(kafkaProducer);

        //触发执行
        env.execute();

    }
}
