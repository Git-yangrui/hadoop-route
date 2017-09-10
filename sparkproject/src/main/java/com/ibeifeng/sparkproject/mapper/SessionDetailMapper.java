package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.SessionDetail;
import org.apache.ibatis.annotations.Insert;

public interface SessionDetailMapper {

    @Insert("insert into session_detail values(#{taskid},#{userid},#{sessionid},#{pageid},#{actionTime},#{searchKeyWord},#{clickCategoryId}" +
            ",#{clickProductId},#{ordercategoryIds},#{orderProductIds},#{payCategoryIds},#{payProductIds})")
    void insert(SessionDetail sessionDetail);
}
