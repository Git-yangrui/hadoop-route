package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:application-test.xml"})
public class ITaskMapperTest {

    @Autowired
    private ITaskMapper iTaskMapper;

    @Test
    public void test_ItaskMapper_findById(){
        Task task = iTaskMapper.findById(1);
    }

}
