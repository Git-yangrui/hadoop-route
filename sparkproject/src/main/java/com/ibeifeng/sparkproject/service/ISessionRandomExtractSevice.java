package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.mapper.ISessionRandomExtractMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service("iSessionRandomExtractSevice")
@Transactional(readOnly = true)
public class ISessionRandomExtractSevice {

    @Autowired
    private ISessionRandomExtractMapper iSessionRandomExtractMapper;

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void insert(SessionRandomExtract sessionRandomExtract){
        iSessionRandomExtractMapper.insert(sessionRandomExtract);
    }
}
