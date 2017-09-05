package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.mapper.ITaskMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service("taskService")
@Transactional(readOnly = true)
public class TaskService {

    @Autowired
    private ITaskMapper iTaskMapper;

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public int insert(Task task){
        int a=iTaskMapper.insertTask(task);
//        int a1=1/0;
        return a;
    }


    public  Task findById(long taskId){
       return iTaskMapper.findById(taskId);
    }


}
