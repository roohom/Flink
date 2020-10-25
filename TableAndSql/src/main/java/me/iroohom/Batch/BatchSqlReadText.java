package me.iroohom.Batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: BatchSqlReadText
 * @Author: Roohom
 * @Function: 求各班级每个学科的平均分、三科总分平均分,先合并数据再查询
 * @Date: 2020/10/25 15:38
 * @Software: IntelliJ IDEA
 */
public class BatchSqlReadText {

    /**
     * 需求：求各班级每个学科的平均分、三科总分平均分
     * 开发步骤：
     * 1.初始化批处理执行环境
     * 2.获取表执行环境
     * 3.加载数据源
     * 4.数据转换，新建bean对象
     * 5.数据关联
     * 6.注册视图
     * 7.查询sql
     * 8.转换成DataSet
     * 9.数据写入文件
     * 10.触发执行
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tblEnv = BatchTableEnvironment.create(env);

        //3.加载数据源
        DataSource<String> scoreSource = env.readTextFile("datas\\sql_scores.txt");
        DataSource<String> studentSource = env.readTextFile("datas\\sql_students.txt");


        //转换Score对象
        MapOperator<String, Score> scoreBean = scoreSource.map(new MapFunction<String, Score>() {
            @Override
            public Score map(String value) throws Exception {
                String[] split = value.split(",");
                return new Score(
                        Integer.valueOf(split[0]),
                        Integer.valueOf(split[1]),
                        Integer.valueOf(split[2]),
                        Integer.valueOf(split[3])
                );
            }
        });

        //转换Student对象
        MapOperator<String, Student> studentBean = studentSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] split = value.split(",");

                return new Student(
                        Integer.valueOf(split[0]),
                        split[1],
                        split[2]);
            }
        });

        //5.数据关联，将Student数据和Score数据进行join
        JoinOperator.EquiJoin<Student, Score, StudentScore> scoreStudentScoreEquiJoin = studentBean.join(scoreBean).where("id").equalTo("id")
                .with(new JoinFunction<Student, Score, StudentScore>() {
                    @Override
                    public StudentScore join(Student first, Score second) throws Exception {
                        return new StudentScore(
                                first.getClassName(),
                                second.getChinese(),
                                second.getMath(),
                                second.getEnglish()
                        );
                    }
                });

        /**
         *  6.注册视图，旧版本中叫做注册表
         *  param1:表名
         *  param2:用于接收结果的数据集
         *  param3:数据集中的字段
         */
        tblEnv.createTemporaryView("tbl", scoreStudentScoreEquiJoin, "className,chinese,math,english");

        //7.查询SQL的书写
        String sql =
                "select className," +
                        "avg(chinese)," +
                        "avg(math)," +
                        "avg(english)," +
                        "avg(chinese+math+english)" +
                        "from tbl " +
                        "group by className";

        //执行查询
        Table table = tblEnv.sqlQuery(sql);
        //table转换成DataSet
        DataSet<Row> rowDataSet = tblEnv.toDataSet(table, Row.class);
        //9.数据写入文件
        rowDataSet.writeAsText("datas\\output\\batchSqlOutput.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //10.触发执行
        env.execute();

    }


    /**
     * 4.新建Bean对象
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Score {
        private Integer id;
        private Integer chinese;
        private Integer math;
        private Integer english;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private String className;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StudentScore {
        private String className;
        private Integer chinese;
        private Integer math;
        private Integer english;
    }

}
