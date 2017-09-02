package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.connect.JdbcTempleteUtil;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.domain.Task;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ITaskDAOImpl implements ITaskDAO {

    @Override
    public Task findById(int taskId) {
        final Task task = new Task();
        JdbcTemplate jdbcTemplate = JdbcTempleteUtil.getJdbcTemplate();
        String sql = "";
        Object[] args = new Object[]{};
        List<Task> tasks = jdbcTemplate.query(sql, new RowMapper<Task>() {
            @Override
            public Task mapRow(ResultSet resultSet, int i) throws SQLException {
                task.setTaskId(resultSet.getLong(1));
                task.setTaskName(resultSet.getString(2));
                task.setCreateTime(resultSet.getString(3));
                task.setStartTime(resultSet.getString(4));
                task.setFinishTime(resultSet.getString(5));
                task.setTaskType(resultSet.getString(6));
                task.setTaskStatus(resultSet.getString(7));
                task.setTaskParam(resultSet.getString(8));
                return task;
            }
        }, args);
        return tasks.get(0);
    }
}
