package me.iroohom;

import lombok.Data;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * @ClassName: JoinDemo
 * @Author: Roohom
 * @Function:
 * @Date: 2020/10/21 16:55
 * @Software: IntelliJ IDEA
 */
public class JoinDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Score> score = env.readCsvFile("datas\\score.csv")
                .pojoType(Score.class, "id", "name", "subId", "score");

        DataSource<Subject> subject = env.readCsvFile("datas\\subject.csv")
                .pojoType(Subject.class, "subId", "subName");

        //join数据关联
        score.join(subject).where("subId").equalTo("subId")
                .with(new JoinFunction<Score, Subject, Object>() {
                    @Override
                    public Object join(Score first, Subject second) throws Exception {
                        return Tuple4.of(first.getId(),first.getName(),second.getSubName(),first.getScore());
                    }
                })
                .print();
    }

    @Data
    public static class Score{
        private Integer id;
        private String name;
        private Integer subId;
        private Double score;
    }

    @Data
    public static class Subject{
        private Integer subId;
        private String subName;
    }
}
