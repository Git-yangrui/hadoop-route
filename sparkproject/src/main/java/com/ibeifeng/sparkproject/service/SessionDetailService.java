package com.ibeifeng.sparkproject.service;

import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.mapper.SessionDetailMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


@Service("sessionDetailService")
@Transactional(readOnly = true)
public class SessionDetailService {

    @Autowired
    private SessionDetailMapper sessionDetailMapper;

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW)
    public void insert(SessionDetail sessionDetail) {

        sessionDetailMapper.insert(sessionDetail);
    }
}
