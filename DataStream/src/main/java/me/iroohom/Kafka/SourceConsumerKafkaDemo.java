package me.iroohom.Kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Properties;

/**
 * @ClassName: SourceConsumerKafkaDemo
 * @Author: Roohom
 * @Function: Flink从Kafka消费
 * @Date: 2020/10/22 11:42
 * @Software: IntelliJ IDEA
 */
public class SourceConsumerKafkaDemo {
    /**
     * 开发步骤：
     * 1.获取流处理执行环境
     * 2.自定义数据源
     *  （1）配置kafka消费参数
     *  //核心配置 ：broker,消费组
     *  （2）flink整合kafka
     *  （3）设置kafka消费起始位置
     *  （4）加载kafka消费数据
     * 3.打印数据
     * 4.触发执行
     */
    public static void main(String[] args) throws Exception {
        //1.获取流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //（1）配置kafka消费参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","node1:9092,node2:9092,node3:9092");
        properties.setProperty("group.id","test");
        //kafka自己管理偏移量，从最近偏移量消费,kafka自己管理偏移量
        properties.setProperty("auto.offset.resst","latest");
        properties.setProperty("flink.partition-discovery.interval-millis","5000");

        //Creates a new Kafka streaming source consumer.
        //(2) flink整合kafka
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        //3）设置kafka消费起始位置
        //从头消费，与kafka的偏移量无关
//        consumer.setStartFromEarliest();

        //从最新偏移量消费，与kafka的偏移量无关，由Flink来管理
//        consumer.setStartFromLatest();

        //默认参数，从最近的偏移量消费（企业中常用）
//        consumer.setStartFromGroupOffsets();
        //从指定时间戳消费
//        consumer.setStartFromTimestamp();

        HashMap<KafkaTopicPartition, Long> map = new HashMap<>();
        map.put(new KafkaTopicPartition("test", 0), 1L);
        map.put(new KafkaTopicPartition("test", 1), 1L);
        map.put(new KafkaTopicPartition("test", 2), 1L);

        //从kafka指定topic的指定分区和偏移量消费数据
        consumer.setStartFromSpecificOffsets(map);

        //(4)加载Kafka消费数据
        DataStreamSource<String> source = env.addSource(consumer);

        //3.打印数据
        source.print();
        //4.触发执行
        env.execute();
    }
}
