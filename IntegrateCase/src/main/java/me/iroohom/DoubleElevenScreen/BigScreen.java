package me.iroohom.DoubleElevenScreen;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @ClassName: BigScreen
 * @Author: Roohom
 * @Function: 模拟实现双11大屏统计，从当前零点开始到当前时间的订单实时统计
 * @Date: 2020/10/26 09:46
 * @Software: IntelliJ IDEA
 */
public class BigScreen {

    /**
     * 开发步骤：
     * 1.初始化环境
     * 2.自定义数据源
     * 3.数据分组，
     * 4.划分时间窗口(按天划分、按秒触发)
     * 5.聚合计算
     * 实现：AggregateFunction、WindowFunction
     * 封装中间结果CategoryPojo(category,totalPrice,dateTime)
     * 6.计算结果
     * （1）时间分组
     * （2）划分时间窗口
     * （3）窗口处理：遍历、排序
     * 7.打印数据
     * 8.触发执行
     */

    public static void main(String[] args) throws Exception {
        //初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //自定义数据源
        DataStreamSource<Tuple2<String, Double>> source = env.addSource(new BigSource());

        //Transformation
        SingleOutputStreamOperator<CategoryPojo> aggregate = source.keyBy(0)
                .timeWindow(Time.days(1), Time.seconds(1))
                .aggregate(new AggregateBigScreen(), new WindowFunctionBigScreen());

        //Process
        SingleOutputStreamOperator<Object> result = aggregate.keyBy("dateTime")
                .timeWindow(Time.seconds(1))
                .apply(new WinFuction());

        result.print();
        env.execute();

    }

    private static class BigSource implements SourceFunction<Tuple2<String, Double>> {
        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            String[] category = {
                    "女装", "男装",
                    "图书", "家电",
                    "洗护", "美妆",
                    "运动", "游戏",
                    "户外", "家具",
                    "乐器", "办公"
            };

            Random random = new Random();
            while (true) {
                for (String s : category) {
                    String randCate = category[(int) (random.nextInt(category.length - 1) * Math.random())];

                    double price = random.nextDouble() * 100;

                    ctx.collect(Tuple2.of(randCate, price));
                }
            }
        }

        @Override
        public void cancel() {

        }
    }

    private static class AggregateBigScreen implements AggregateFunction<Tuple2<String, Double>, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0D;
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double accumulator) {
            return value.f1 + accumulator;
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator;
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b;
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CategoryPojo {
        private String category;
        private Double totalPrice;
        private String dateTime;
    }


    private static class WindowFunctionBigScreen implements WindowFunction<Double, CategoryPojo, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Double> input, Collector<CategoryPojo> out) throws Exception {
            Double totalPrice = input.iterator().next();
            String category = ((Tuple1<String>) tuple).f0;

            CategoryPojo categoryPojo = new CategoryPojo();
            categoryPojo.setTotalPrice(totalPrice);
            categoryPojo.setCategory(category);
            String date = DateFormatUtils.format(new Date(System.currentTimeMillis()), "yyyy-MM-dd HH:mm:ss");
            categoryPojo.setDateTime(date);
            out.collect(categoryPojo);
        }
    }

    /**
     * 遍历，排序，优先级队列
     */
    private static class WinFuction implements WindowFunction<CategoryPojo, Object, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<CategoryPojo> input, Collector<Object> out) throws Exception {
            //新建优先级队列,3表示队列的长度为3
            PriorityQueue<CategoryPojo> queue = new PriorityQueue<>(3, new Comparator<CategoryPojo>() {

                /**
                 * 不能全局排序 如果想要全局排序，需要先转成数组
                 * @param o1
                 * @param o2
                 * @return
                 */
                @Override
                public int compare(CategoryPojo o1, CategoryPojo o2) {
                    return o1.getTotalPrice() >= o2.getTotalPrice() ? 1 : -1;
                }
            });

            //定义所有商品分类的销售总额
            Double totalCategoryPrice = 0D;

            for (CategoryPojo categoryPojo : input) {
                //实现Top3，把数据加载到优先级队列
                if (queue.size() < 3) {
                    queue.add(categoryPojo);
                } else {
                    CategoryPojo peek = queue.peek();
                    if (peek.getTotalPrice() < categoryPojo.getTotalPrice()) {
                        queue.poll();
                        queue.add(categoryPojo);
                    }
                }


                totalCategoryPrice += categoryPojo.getTotalPrice();
            }
            //queue全局排序
            List<String> collect = queue.stream().sorted(new Comparator<CategoryPojo>() {
                @Override
                public int compare(CategoryPojo o1, CategoryPojo o2) {
                    return o1.getTotalPrice() >= o2.getTotalPrice() ? 1 : -1;
                }
            })
                    //使用map转换，使得打印更清晰
                    .map(line -> "分类:" + line.getCategory() + ",销售额:" + line.getTotalPrice())
                    .collect(Collectors.toList());

            String result = "所有商品销售总额:" + totalCategoryPrice + ",\n\t Top3 " +collect;
            out.collect(result);

        }
    }
}
