package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:application-core.xml"})
public class ITaskServiceTest {

    @Autowired
    private TaskService taskService;
    @Test
    public void test_transaction(){
        Task task = taskService.findById(1);
        Task task1 = new Task();
        task1.setTaskName(task.getTaskName());
        task1.setTaskParam(task.getTaskParam());
        task1.setTaskStatus(task.getTaskStatus());
        task1.setTaskType(task.getTaskType());
        task1.setFinishTime(task.getFinishTime());
        taskService.insert(task1);
    }
}
