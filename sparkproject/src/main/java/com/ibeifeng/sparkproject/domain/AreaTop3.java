package com.ibeifeng.sparkproject.domain;

import java.io.Serializable;

public class AreaTop3 implements Serializable {

    private long taskid;

    private String area;

    private String areaLevel;

    private long productId;

    private String cityNames;

    private long cliclCount;

    private String productName;

    private String productStatus;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getAreaLevel() {
        return areaLevel;
    }

    public void setAreaLevel(String areaLevel) {
        this.areaLevel = areaLevel;
    }



    public String getCityNames() {
        return cityNames;
    }

    public void setCityNames(String cityNames) {
        this.cityNames = cityNames;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public long getCliclCount() {
        return cliclCount;
    }

    public void setCliclCount(long cliclCount) {
        this.cliclCount = cliclCount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductStatus() {
        return productStatus;
    }

    public void setProductStatus(String productStatus) {
        this.productStatus = productStatus;
    }
}
