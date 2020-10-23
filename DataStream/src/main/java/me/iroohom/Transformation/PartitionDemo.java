package me.iroohom.Transformation;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: PartitionDemo
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/22 14:45
 * @Software: IntelliJ IDEA
 */
public class PartitionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, String>> source = env.fromElements(Tuple2.of(1, "1"), Tuple2.of(2, "2"),Tuple2.of(3, "3"), Tuple2.of(4, "4"));

        DataStream<Tuple2<Integer, String>> rs = source.global();
        DataStream<Tuple2<Integer, String>> broadcast = source.broadcast();
        DataStream<Tuple2<Integer, String>> forward = source.forward();
        DataStream<Tuple2<Integer, String>> shuffle = source.shuffle();
        DataStream<Tuple2<Integer, String>> rebalance = source.rebalance();
        DataStream<Tuple2<Integer, String>> rescale = source.rescale();

        DataStream<Tuple2<Integer, String>> partitionCustom = source.partitionCustom(new Partitioner<Object>() {
            @Override
            public int partition(Object key, int numPartitions) {
                return key.hashCode() % 2;
            }
        }, 1);


//        rs.print();
        broadcast.print();
//        forward.print();
//        shuffle.print(); //随机分配
//        rebalance.print(); //均匀分配
//        rescale.print();
//        partitionCustom.print();

        env.execute();


    }
}
