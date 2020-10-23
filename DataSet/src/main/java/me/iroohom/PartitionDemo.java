package me.iroohom;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: PartitionDemo
 * @Author: Roohom
 * @Function: 分区
 * @Date: 2020/10/21 17:43
 * @Software: IntelliJ IDEA
 */
public class PartitionDemo {
    /**
     * 1、hash分区
     * 2、区间范围分区
     * 3、自定义分区
     * 4、排序分期
     */
    public static void main(String[] args) throws Exception {
        List<Tuple3<Integer, Long, String>> list = new ArrayList<>();
        list.add(Tuple3.of(1, 1L, "Hello"));
        list.add(Tuple3.of(2, 2L, "Hello"));
        list.add(Tuple3.of(3, 2L, "Hello"));
        list.add(Tuple3.of(4, 3L, "Hello"));
        list.add(Tuple3.of(5, 3L, "Hello"));
        list.add(Tuple3.of(6, 3L, "hehe"));
        list.add(Tuple3.of(7, 4L, "hehe"));
        list.add(Tuple3.of(8, 4L, "hehe"));
        list.add(Tuple3.of(9, 4L, "hehe"));
        list.add(Tuple3.of(10, 4L, "hehe"));
        list.add(Tuple3.of(11, 5L, "hehe"));
        list.add(Tuple3.of(12, 5L, "hehe"));
        list.add(Tuple3.of(13, 5L, "hehe"));
        list.add(Tuple3.of(14, 5L, "hehe"));
        list.add(Tuple3.of(15, 5L, "hehe"));
        list.add(Tuple3.of(16, 6L, "hehe"));
        list.add(Tuple3.of(17, 6L, "hehe"));
        list.add(Tuple3.of(18, 6L, "hehe"));
        list.add(Tuple3.of(19, 6L, "hehe"));
        list.add(Tuple3.of(20, 6L, "hehe"));
        list.add(Tuple3.of(21, 6L, "hehe"));

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Integer, Long, String>> source = env.fromCollection(list);
        //1.Hash分区，根据指定字段的HASH值
//        source.partitionByHash(0).writeAsText("hash", FileSystem.WriteMode.OVERWRITE);

        //2.区间范围分区
//        source.partitionByRange(0).writeAsText("range", FileSystem.WriteMode.OVERWRITE);

        //3.自定义分区 参数1：自定义分区方法，2：指定字段进行自定义分区
//        source.partitionCustom(new Partitioner<Object>() {
//            @Override
//            public int partition(Object key, int numPartitions) {
//                return key.hashCode() % 2;
//            }
//        },1).writeAsText("custom", FileSystem.WriteMode.OVERWRITE);

        //4.排序分区,默认生成一个排序文件
        source.sortPartition(0, Order.DESCENDING).writeAsText("sort", FileSystem.WriteMode.OVERWRITE).setParallelism(2);


        //触发执行
        env.execute();


    }
}
