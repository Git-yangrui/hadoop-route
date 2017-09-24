package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.AreaTop3;
import com.ibeifeng.sparkproject.mapper.AreTopsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service("AreTop3Serive")
@Transactional(readOnly = true)
public class AreTop3Serive {

    @Autowired
    private AreTopsMapper areTopsMapper;

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void inserAll(List<AreaTop3> lists){
        areTopsMapper.insertAll(lists);
    }
}
