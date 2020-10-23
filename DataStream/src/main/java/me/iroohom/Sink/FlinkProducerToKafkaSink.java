package me.iroohom.Sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * @ClassName: FlinkProducerToKafkaSink
 * @Author: Roohom
 * @Function: 将Socket流中的的数据通过自定义Sink保存到Kafka
 * @Date: 2020/10/22 18:23
 * @Software: IntelliJ IDEA
 */
public class FlinkProducerToKafkaSink {

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

        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.加载数据源，加载nc
        DataStreamSource<String> source = env.socketTextStream("node2", 8090);
        //3.配置kafka生产参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092");
        //4.获取kafka生产者对象
        FlinkKafkaProducer test2 = new FlinkKafkaProducer("test2", new SimpleStringSchema(), properties);
        //5.数据写入kafka
        source.addSink(test2);
        //触发执行
        env.execute();


    }
}
