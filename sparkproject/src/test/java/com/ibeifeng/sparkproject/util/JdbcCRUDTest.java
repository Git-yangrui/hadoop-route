package com.ibeifeng.sparkproject.util;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

@Ignore
public class JdbcCRUDTest {

    @Test
    public void insert() {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",  "root");
            stmt =conn.createStatement();
            String sql = "insert into test_user(name,age) values('李四',26)";
            int rtn = stmt.executeUpdate(sql);
            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    @Test
    public void update() {
        Connection conn = null;
        Statement stmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "root");
            stmt = conn.createStatement();

            String sql = "update test_user set age=27 where name='李四'";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

   @Test
    public void delete() {
        Connection conn = null;
        Statement stmt = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "root");
            stmt = conn.createStatement();
            String sql = "delete from test_user where name='李四'";
            int rtn = stmt.executeUpdate(sql);
            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    @Test
    public void select() {
        Connection conn = null;
        Statement stmt = null;
        // 对于select查询语句，需要定义ResultSet
        // ResultSet就代表了，你的select语句查询出来的数据
        // 需要通过ResutSet对象，来遍历你查询出来的每一行数据，然后对数据进行保存或者处理
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "root");
            stmt = conn.createStatement();

            String sql = "select * from test_user";
            rs = stmt.executeQuery(sql);

            // 获取到ResultSet以后，就需要对其进行遍历，然后获取查询出来的每一条数据
            while (rs.next()) {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                int age = rs.getInt(3);
                System.out.println("id=" + id + ", name=" + name + ", age=" + age);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    @Test
    public void preparedStatement() {
        Connection conn = null;
        /**
         * 如果使用Statement，那么就必须在SQL语句中，实际地去嵌入值，比如之前的insert语句
         *
         * 但是这种方式有一个弊端，第一，是容易发生SQL注入，SQL注入，简单来说，就是，你的网页的用户
         * 在使用，比如论坛的留言板，电商网站的评论页面，提交内容的时候，可以使用'1 or 1'，诸如此类的
         * 非法的字符，然后你的后台，如果在插入评论数据到表中的时候，如果使用Statement，就会原封不动的
         * 将用户填写的内容拼接在SQL中，此时可能会发生对数据库的意外的损坏，甚至数据泄露，这种情况就叫做
         * SQL注入
         *
         * 第二种弊端，就是性能的低下，比如insert into test_user(name,age) values('张三',25)
         * insert into test_user(name,age) values('李四',26)
         * 其实两条SQL语句的结构大同小异，但是如果使用这种方式，在MySQL中执行SQL语句的时候，却需要对
         * 每一条SQL语句都实现编译，编译的耗时在整个SQL语句的执行耗时中占据了大部分的比例
         * 所以，Statement会导致执行大量类似SQL语句的时候的，性能低下
         *
         * 如果使用PreparedStatement，那么就可以解决上述的两个问题
         * 1、SQL注入，使用PreparedStatement时，是可以在SQL语句中，对值所在的位置使用?这种占位符的
         * 使用占位符之后，实际的值，可以通过另外一份放在数组中的参数来代表。此时PreparedStatement会对
         * 值做特殊的处理，往往特殊处理后，就会导致不法分子的恶意注入的SQL代码失效
         * 2、提升性能，使用PreparedStatement之后，其实结构类似的SQL语句，都变成一样的了，因为值的地方
         * 都会变成?，那么一条SQL语句，在MySQL中只会编译一次，后面的SQL语句过来，就直接拿编译后的执行计划
         * 加上不同的参数直接执行，可以大大提升性能
         */
        PreparedStatement pstmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark-project?characterEncoding=utf8",
                    "root",
                    "root");

            // 第一个，SQL语句中，值所在的地方，都用问好代表
            String sql = "insert into test_user(name,age) values(?,?)";

            pstmt = conn.prepareStatement(sql);

            // 第二个，必须调用PreparedStatement的setX()系列方法，对指定的占位符设置实际的值
            pstmt.setString(1, "李四");
            pstmt.setInt(2, 26);

            // 第三个，执行SQL语句时，直接使用executeUpdate()即可，不用传入任何参数
            int rtn = pstmt.executeUpdate();

            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

}
