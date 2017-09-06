package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import org.apache.ibatis.annotations.Insert;

public interface ISessionRandomExtractMapper {
    /**
     * 插入session随机抽取
     * @param sessionAggrStat
     */
//    private long taskid;
//    private String sessionid;
//
//    private String startTime;
//    private String searchKeywords;
//    private String clickCategoryIds;
    @Insert("insert into session_random_extract values(#{taskid},#{sessionid},#{startTime},#{searchKeywords},#{clickCategoryIds})")
    void insert(SessionRandomExtract sessionRandomExtract);


}
