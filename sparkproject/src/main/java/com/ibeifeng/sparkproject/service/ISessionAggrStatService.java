package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.mapper.ISessionAggrStatMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service("iSessionAggrStatService")
@Transactional(readOnly = true)
public class ISessionAggrStatService {

    @Autowired
    private ISessionAggrStatMapper iSessionAggrStatMapper;

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void insert(SessionAggrStat sessionAggrStat) {
        iSessionAggrStatMapper.insert(sessionAggrStat);
    }
}
