package me.iroohom.AsyncIO;

import java.sql.*;

/**
 * @author roohom
 * @Date 2020/10/18
 */
public class DbUtil {

    private static Connection connection;
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/roohom?autoReconnect=true&useSSL=false&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai";
    private static final String USER = "root";
    private static final String PASSWORD = "123456";

    static {
        init();
    }

    private static void init() {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Driver not found!" + e.getMessage());
        }
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
        } catch (SQLException e) {
            System.out.println("init connection failed!" + e.getMessage());
        }
    }

    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("close connection failed!" + e.getMessage());
        }
    }

    public static AsyncReadMySqlMethod2.CategoryInfo query(AsyncReadMySqlMethod2.CategoryInfo category) {
        try {
            String sql = "select id,name from t_category where id = "+ category.getId();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs != null && rs.next()) {
                category.setName(rs.getString("name"));
            }
        } catch (SQLException e) {
            System.out.println("query failed!" + e.getMessage());
        }
        return category;
    }
}
