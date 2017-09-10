package com.ibeifeng.sparkproject.domain;

import java.io.Serializable;

public class SessionDetail implements Serializable {


    private long taskid;

    private long userid;

    private String sessionid;

    private long pageid;

    private String actionTime;

    private String searchKeyWord;

    private long clickCategoryId;

    private long clickProductId;


    private String ordercategoryIds;

    private String orderProductIds;

    private String payCategoryIds;

    private String payProductIds;

    public String getPayProductIds() {
        return payProductIds;
    }

    public void setPayProductIds(String payProductIds) {
        this.payProductIds = payProductIds;
    }

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public long getPageid() {
        return pageid;
    }

    public void setPageid(long pageid) {
        this.pageid = pageid;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyWord() {
        return searchKeyWord;
    }

    public void setSearchKeyWord(String searchKeyWord) {
        this.searchKeyWord = searchKeyWord;
    }

    public long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public long getClickProductId() {
        return clickProductId;
    }

    public void setClickProductId(long clickProductId) {
        this.clickProductId = clickProductId;
    }


    public String getOrdercategoryIds() {
        return ordercategoryIds;
    }

    public void setOrdercategoryIds(String ordercategoryIds) {
        this.ordercategoryIds = ordercategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }
}
