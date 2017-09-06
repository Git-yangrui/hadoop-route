package com.ibeifeng.sparkproject.mapper;

import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import org.apache.ibatis.annotations.Insert;

public interface ISessionAggrStatMapper {
    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    @Insert("insert into session_aggr_stat values(#{taskid},#{session_count},#{visit_length_1s_3s_ratio},#{visit_length_4s_6s_ratio},#{visit_length_7s_9s_ratio},#{visit_length_10s_30s_ratio}," +
            "#{visit_length_30s_60s_ratio},#{visit_length_1m_3m_ratio},#{visit_length_3m_10m_ratio},#{visit_length_10m_30m_ratio},#{visit_length_30m_ratio},#{step_length_1_3_ratio}," +
            "#{step_length_4_6_ratio},#{step_length_7_9_ratio},#{step_length_10_30_ratio},#{step_length_30_60_ratio},#{step_length_60_ratio})")
    void insert(SessionAggrStat sessionAggrStat);

}
