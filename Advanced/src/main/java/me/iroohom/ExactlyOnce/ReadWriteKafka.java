package me.iroohom.ExactlyOnce;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * @ClassName: ReadWriteKafka
 * @Author: Roohom
 * @Function: 端对端一次性语义读写kafka End to End Exactly Once
 * @Date: 2020/10/25 11:40
 * @Software: IntelliJ IDEA
 */
public class ReadWriteKafka {
    /**
     * 需求：消费kafka数据再写入kafka ,实现端到端得一次性语义
     * 开发步骤：
     * 1.初始化环境
     * 2.设置检查点
     * 3.配置kafka消费参数
     * 4.获取kafka数据
     * 5.获取生产者对象，设置一次性语义
     * 6.生产数据
     * 7.触发执行
     */
    public static void main(String[] args) throws Exception {
        //1.初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置检查点
        env.enableCheckpointing(5000L);
        env.setStateBackend(new FsStateBackend("file:///checkpoint"));
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置最大线程数为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //3.配置kafka消费参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        //从头消费
        consumer.setStartFromEarliest();
        //4.获取Kafka数据
        DataStreamSource<String> source = env.addSource(consumer);
        source.print("Kafka:>>>>");

        //5.配置生产者参数，获取生产者对象，设置一次性语义
        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        //设置事务超时时间，也可在kafka配置中设置
        properties1.setProperty("transaction.timeout.ms", 60000 * 15 + "");
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer<>("test2", new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), properties1, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        //6.生产数据
        source.addSink(kafkaProducer);
        env.execute();


    }
}
