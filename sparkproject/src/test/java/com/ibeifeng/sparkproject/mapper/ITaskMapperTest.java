package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:application-core.xml"})
public class ITaskMapperTest {

    @Autowired
    private ITaskMapper iTaskMapper;

    @Test
    public void test_ItaskMapper_findById(){
        Task task = iTaskMapper.findById(1);
    }
    @Test
    public void test_ItaskMapper_insert(){
        Task task = iTaskMapper.findById(1);
        Task task1 = new Task();
        task1.setTaskName(task.getTaskName());
        task1.setTaskParam(task.getTaskParam());
        task1.setTaskStatus(task.getTaskStatus());
        task1.setTaskType(task.getTaskType());
        task1.setFinishTime(task.getFinishTime());
        iTaskMapper.insertTask(task1);
    }

}
