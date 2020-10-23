package me.iroohom;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName: DistributedCacheDemo
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/21 18:48
 * @Software: IntelliJ IDEA
 */
public class DistributedCacheDemo {

    /**
     * 将scoreDS(学号, 学科, 成绩)中的数据和分布式缓存中的数据(学号,姓名)关联,得到这样格式的数据: (学生姓名,学科,成绩)
     */

    public static void main(String[] args) throws Exception {
        //scoreDS：Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86))

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Integer, String, Integer>> scoreSource = env.fromCollection(Arrays.asList(Tuple3.of(1, "语文", 50), Tuple3.of(2, "数学", 70), Tuple3.of(3, "英文", 86)));

        env.registerCachedFile("file:///E:\\IDEAJ\\Flink\\datas\\distribute_cache_student", "cache");
        scoreSource.map(new RichMapFunction<Tuple3<Integer, String, Integer>, Object>() {
            List<String> list;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取分布式缓存文件
                File cache = getRuntimeContext().getDistributedCache().getFile("cache");
                //FileUtils解析
                list = FileUtils.readLines(cache);
            }

            @Override
            public Object map(Tuple3<Integer, String, Integer> value) throws Exception {
                for (String s : list) {
                    String[] split = s.split(",");
                    if (Integer.valueOf(split[0]).equals(value.f0)) {
                        return Tuple3.of(split[1], value.f1, value.f2);
                    }
                }
                return null;
            }
        }).print();


    }
}
