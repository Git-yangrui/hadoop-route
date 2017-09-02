package com.ibeifeng.sparkproject.connect;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.mchange.v2.c3p0.DriverManagerDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

public class JdbcTempleteUtil {

    public static DataSource dataSource;

    public static JdbcTemplate jdbcTemplate;

    public static JdbcTemplate getJdbcTemplete(DataSource dataSource) {
        if (jdbcTemplate == null) {
            jdbcTemplate = new JdbcTemplate(dataSource);
        }
        return jdbcTemplate;
    }

    public static DataSource getDataSource() {
        if (dataSource == null) {
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setJdbcUrl(ConfigurationManager.getProperty("dburl"));
            dataSource.setUser(ConfigurationManager.getProperty("username"));
            dataSource.setPassword(ConfigurationManager.getProperty("password"));
            dataSource.setDriverClass(ConfigurationManager.getProperty("dirverClass"));
            return dataSource;
//            dirverClass=com.mysql.jdbc.Driver
//            username=root
//            password=root
//            dburl=jdbc:mysql://yangziyu01:3306/spark-project?createDatabaseIfNotExist=true
        }
        return dataSource;
    }

    public static JdbcTemplate getJdbcTemplate(){
        if(jdbcTemplate==null){
            DataSource dataSource = getDataSource();
            jdbcTemplate=new JdbcTemplate(dataSource);
        }
        return jdbcTemplate;
    }

}
