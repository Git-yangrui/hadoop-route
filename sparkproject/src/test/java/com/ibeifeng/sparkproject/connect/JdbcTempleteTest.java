package com.ibeifeng.sparkproject.connect;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcTempleteTest {
    @Test
    public void test_connect(){
        JdbcTemplate jdbcTemplate = JdbcTempleteUtil.getJdbcTemplate();
        jdbcTemplate.execute("SELECT * from session_aggr_stat;\n");
    }
}
