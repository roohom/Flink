package me.iroohom.Batch;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * @ClassName: BatchSqlReadCsvText
 * @Author: Roohom
 * @Function: 求各班级每个学科的平均分，三科总平均分，使用CsvTableSource方式
 * @Date: 2020/10/25 15:58
 * @Software: IntelliJ IDEA
 */
public class BatchSqlReadCsvText {
    /**
     * 需求：求各班级每个学科的平均分、三科总分平均分
     * （1）通过CsvTableSource构建数据源
     * （2）注册表tblEnv.registerTableSource("score",score);
     */
    public static void main(String[] args) throws Exception {
        //1.获取批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2.获取批处理表执行环境
        BatchTableEnvironment tblEnv = BatchTableEnvironment.create(env);

        //3.加载数据源，构建Score
        CsvTableSource scores = CsvTableSource.builder()
                .path("datas\\sql_scores.txt")
                .field("id", Types.INT)
                .field("chinese", Types.INT)
                .field("math", Types.INT)
                .field("english", Types.INT)
                .build();


        //3.加载数据源，构建Student
        CsvTableSource students = CsvTableSource.builder()
                .path("datas\\sql_students.txt")
                .field("id", Types.INT)
                .field("name", Types.STRING)
                .field("className", Types.STRING)
                .build();

        //注册scores和students表，Registered tables can be referenced in SQL queries.已经注册过的表才可以使用SQL查询
        tblEnv.registerTableSource("scores",scores);
        tblEnv.registerTableSource("students",students);

        //执行SQL查询
        Table table = tblEnv.sqlQuery("select name,className,chinese,math,english from students t1 join scores t2 on t1.id = t2.id");
        //注册将scores和student经过join之后的结果集表
        tblEnv.registerTable("t_student_score",table);


        //SQL语句
        String sql =
                "SELECT className, AVG(chinese) as avg_chinese, AVG(math) as avg_math, AVG(english) as avg_english, " +
                "AVG(chinese + math + english) as avg_total " +
                "FROM t_student_score " +
                "GROUP BY className " +
                "ORDER BY avg_total";

        //执行最终SQL查询
        Table table1 = tblEnv.sqlQuery(sql);
        //将table结果转换成dataSet结果，不想使用指定的结果Bean来接收，则可以使用Row来接收，理论上可以接收任何类型
        DataSet<Row> rowDataSet = tblEnv.toDataSet(table1, Row.class);

        //Sink 将结果保存在本地
        rowDataSet.writeAsText("datas\\output\\batchSqlCsvTableOutput.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();

        //结果打印，print在此是触发算子
//        rowDataSet.print();

    }
}
