package me.iroohom.Sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName: SinkMySqlDemo
 * @Author: Roohom
 * @Function: 使用Sink往MySQL写数据
 * @Date: 2020/10/22 15:06
 * @Software: IntelliJ IDEA
 */
public class SinkMySqlDemo {
    /**
     * 1.初始化环境
     * 2.加载数据源
     * 3.数据写入
     * （1）初始化连接对象
     * （2）执行写入
     * （3）关流
     * 4.触发执行
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> source = env.fromElements(new Student(12, "张三", 22));
        source.addSink(new MySqlSinkFunction());
        env.execute();


    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

    private static class MySqlSinkFunction extends RichSinkFunction<Student> {
        Connection conn = null;
        PreparedStatement preparedStatement = null;

        //初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            //注册驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            //建立连接
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/roohom?autoReconnect=true&useSSL=false&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai", "root", "123456");
            preparedStatement = conn.prepareStatement("insert into t_student values(?,?,?)");

        }

        //初始化执行方法
        @Override
        public void invoke(Student value, Context context) throws Exception {
            //数据插入
            preparedStatement.setInt(1, value.getId());
            preparedStatement.setString(2, value.getName());
            preparedStatement.setInt(3, value.getAge());
            preparedStatement.executeUpdate();
        }

        //连接对象关流
        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
