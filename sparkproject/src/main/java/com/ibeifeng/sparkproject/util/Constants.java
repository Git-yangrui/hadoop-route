package com.ibeifeng.sparkproject.util;

/**
 * 常量接口
 *
 * @author Administrator
 */
public interface Constants {

    /**
     * 数据库相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";

    //作业
    String USERVISI_TSESSIONANALYZER_SPARK = "UserVisitSessionAnalyzerSpark";
    String SPARKSTREMINGAPPLICATION = "SparkStremingApplication";
    String FIELD_SESSIONID = "sessionid";
    String FIELD_STARTTIME="startTime";
    String FIELD_PAY_COUNT="payCount";
    String FIELD_CLICK_COUNT="clickCount";
    String FIELD_VISIT_LENGTH="visitLength";
    String FIELD_STEP_LENGTH="stepLength";
    String FIELD_CATEGORY_ID="categoryid";
    String FIELD_SEARCH_KEYWORDS = "searchkeywords";
    String FIELD_CATEGORY_IDS = "clickcategoryids";
    String FIELD_AGE = "age";
    String FIELD_PROFESSINAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_ORDER_COUNT="orderCount";
    String SPARK_LOCAL = "spark.local";


    String START_DATE = "startDate";
    String END_DATE = "endDate";
    String START_AGE = "startAge";
    String END_AGE = "endAge";
    String PARAM_SEX = "sex";
    String PARAM_PROFESSIONALs = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORYIDS = "categoryIds";



    String SESSION_COUNT ="session_count";
    String TIME_PERIOD_1s_3s="time_period_1s_3s";
    String TIME_PERIOD_4s_6s ="time_period_4s_6s";
            String TIME_PERIOD_7s_9s ="time_period_7s_9s";
            String TIME_PERIOD_10s_30s ="time_period_10s_30s";
            String TIME_PERIOD_30s_60s ="time_period_30s_60s";
            String TIME_PERIOD_1m_3m ="time_period_1m_3m";
            String TIME_PERIOD_3m_10m ="time_period_3m_10m";
            String TIME_PERIOD_10m_30m ="time_period_10m_30m";
            String TIME_PERIOD_30m ="time_period_30m";
            String STEP_PERIOD_1_3 ="step_period_1_3";
            String STEP_PERIOD_4_6 ="step_period_4_6";
            String STEP_PERIOD_7_9 ="step_period_7_9";
            String STEP_PERIOD_10_30 ="step_period_10_30";
            String STEP_PERIOD_30_60 ="step_period_30_60";
            String STEP_PERIOD_60 ="step_period_60";

}
