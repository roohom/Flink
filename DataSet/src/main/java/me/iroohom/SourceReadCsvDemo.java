package me.iroohom;

import lombok.Data;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * @ClassName: SourceReadCsvDemo
 * @Author: Roohom
 * @Function: 如名
 * @Date: 2020/10/21 10:48
 * @Software: IntelliJ IDEA
 */
public class SourceReadCsvDemo {

    @Data
    public static class Subject {
        private Integer id;
        private String name;
    }


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.readCsvFile("datas\\subject.csv")
                .lineDelimiter("\n")
//                .ignoreFirstLine() //删除首行数据，因为一般是标题
                .fieldDelimiter(",") //字段之间的分隔符，默认是逗号
                .pojoType(Subject.class, "id", "name")
                .print();
    }
}
