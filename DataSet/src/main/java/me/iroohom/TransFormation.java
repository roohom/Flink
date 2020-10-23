package me.iroohom;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @ClassName: TransFormation
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/21 11:41
 * @Software: IntelliJ IDEA
 */
public class TransFormation {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.readTextFile("datas\\click.log");

        //(1) map
        MapOperator<String, Click> mapOperator = source.map(new MapFunction<String, Click>() {
            @Override
            public Click map(String s) throws Exception {
                Click click = JSON.parseObject(s, Click.class);
                return click;
            }
        });

        FlatMapOperator<Click, Tuple2<String, Integer>> flatMap = mapOperator.flatMap(new FlatMapFunction<Click, Tuple2<String, Integer>>() {
            //(2) flatMap
            @Override
            public void flatMap(Click click, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Long entryTime = click.getEntryTime();
                String hh = DateFormatUtils.format(entryTime, "yyyy-MM-dd-HH");
                String dd = DateFormatUtils.format(entryTime, "yyyy-MM-dd");
                String mm = DateFormatUtils.format(entryTime, "yyyy-MM");
                collector.collect(Tuple2.of(hh, 1));
                collector.collect(Tuple2.of(dd, 1));
                collector.collect(Tuple2.of(mm, 1));
            }
        });
        //(3) filter
        //过滤出clickLog中使用“谷歌浏览器”访问的日志
//        mapOperator.filter(new FilterFunction<Click>() {
//            @Override
//            public boolean filter(Click click) throws Exception {
//                 return "谷歌浏览器".equals(click.getBrowserType());
//            }
//        }).print();

        mapOperator.map(new MapFunction<Click, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Click click) throws Exception {
                return Tuple2.of(click.getBrowserType(), 1);
            }
            //(4) groupBy
        }).groupBy(0)
                //(5) sum
//                .sum(1)
                //(6) min/minBy
//                .min(1)
//                .minBy(1)
//                .max(1)
//                .maxBy(1)
                //等同于max（min）
//                .aggregate(Aggregations.MAX,1)
                //(7) reduce和reduceGroup
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
//                        return Tuple2.of(stringIntegerTuple2.f0,stringIntegerTuple2.f1+t1.f1);
//                    }
//                }).print();
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String key = null;
                        Integer cnt = 0;
                        for (Tuple2<String, Integer> stringIntegerTuple2 : iterable) {
                            key = stringIntegerTuple2.f0;
                            cnt += stringIntegerTuple2.f1;
                        }
                        collector.collect(Tuple2.of(key, cnt));
                    }
                });

        //(8) 数据的合并union 不会对数据进行去重
        DataSource<String> source2 = env.readTextFile("datas\\click2.log");
        UnionOperator<String> unionOperator = source.union(source2);
//        System.out.println(unionOperator.count());
        //(9) distinct
        mapOperator.map(new MapFunction<Click, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Click value) throws Exception {
                return Tuple2.of(value.getBrowserType(), 1);
            }
        }).distinct(0)
                .print();


    }


    @Data
    public static class Click {
        private String browserType;
        private String cateoryID;
        private Integer channelID;
        private String city;
        private String country;
        private Long entryTime;
        private Long leaveTime;
        private String network;
        private Integer produceID;
        private String privince;
        private String source;
        private Integer userID;
    }

}
