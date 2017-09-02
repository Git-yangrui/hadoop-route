package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

public interface ITaskDAO {
  Task findById(int taskId);
}
