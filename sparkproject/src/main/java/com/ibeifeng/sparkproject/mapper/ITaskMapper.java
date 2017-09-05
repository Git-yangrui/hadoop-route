package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.Task;
import org.apache.ibatis.annotations.*;

public interface ITaskMapper {

    @Select("select * from task where task_id=#{taskId}")
    @Results({
            @Result(id = true, column = "task_id", property = "taskId"),
            @Result(column = "task_name", property = "taskName"),
            @Result(column = "create_time", property = "createTime"),
            @Result(column = "start_time", property = "startTime"),
            @Result(column = "finish_time", property = "finishTime"),
            @Result(column = "task_type", property = "taskType"),
            @Result(column = "task_status", property = "taskStatus"),
            @Result(column = "task_param", property = "taskParam")

    })
    Task findById(@Param("taskId")long taskId);

    @Insert("insert into task values(null,#{taskName},#{createTime},#{startTime},#{finishTime},#{taskType},#{taskStatus},#{taskParam})")
    @Options(useGeneratedKeys = true, keyProperty = "task_id")
    int insertTask(Task task);
}
