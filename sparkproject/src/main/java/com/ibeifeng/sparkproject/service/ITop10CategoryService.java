package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.Top10Catefory;
import com.ibeifeng.sparkproject.mapper.ITop10CategoryMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service("iTop10CategoryService")
@Transactional(readOnly = true)
public class ITop10CategoryService {

    @Autowired
    private ITop10CategoryMapper iTop10CategoryMapper;


    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void insert(Top10Catefory top10Catefory){

        iTop10CategoryMapper.insert(top10Catefory);
    }


}
