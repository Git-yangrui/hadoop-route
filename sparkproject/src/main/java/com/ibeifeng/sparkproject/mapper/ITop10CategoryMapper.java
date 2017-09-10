package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.Top10Catefory;
import org.apache.ibatis.annotations.Insert;

public interface ITop10CategoryMapper {



    @Insert("insert into top10_category values(#{taskid},#{categoryid},#{clickCount},#{orderCount},#{payCount})")
    void insert(Top10Catefory top10Catefory);
}
