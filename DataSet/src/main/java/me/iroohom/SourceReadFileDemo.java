package me.iroohom;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

/**
 * @ClassName: SourceReadFileDemo
 * @Author: Roohom
 * @Function: 如名
 * @Date: 2020/10/21 11:02
 * @Software: IntelliJ IDEA
 */
public class SourceReadFileDemo {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件
//        Configuration configuration = new Configuration();
//        configuration.setBoolean("recursive.file.enumeration",true);
//        env.readTextFile("datas\\dir\\words1.txt")
//                .withParameters(configuration)
//                .print();

        //读取压缩文件
//        env.readTextFile("filePath")
//                .print();


    }

}
