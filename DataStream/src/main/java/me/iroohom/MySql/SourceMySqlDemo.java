package me.iroohom.MySql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName: SourceMySqlDemo
 * @Author: Roohom
 * @Function: 自定义MySQL数据源，从MySQL读取数据
 * @Date: 2020/10/22 10:45
 * @Software: IntelliJ IDEA
 */
public class SourceMySqlDemo {
    /**
     * 需求：自定义读取mysql数据源
     * 1.获取流处理执行环境
     * 2.自定义数据源
     *  （1）初始化mysql连接
     *  （2）读取数据
     *  （3）关流
     * 3.打印数据
     * 4.触发执行
     */

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new ReadMySql()).print();
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


    private static class ReadMySql extends RichSourceFunction<Student> {
        Connection conn = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            //注册驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            //建立连接
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/roohom?autoReconnect=true&useSSL=false&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai", "root", "123456");
            preparedStatement = conn.prepareStatement("select * from t_student");
            resultSet = preparedStatement.executeQuery();
        }

        @Override
        public void run(SourceContext<Student> sourceContext) throws Exception {
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String name = resultSet.getString(2);
                Integer age = resultSet.getInt(3);
                sourceContext.collect(new Student(id, name, age));
            }

        }

        @SneakyThrows
        @Override
        public void cancel() {

        }

        @Override
        public void close() throws Exception {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
