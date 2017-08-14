package com.yangrui;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public abstract class MySparkContext {
    protected  SparkConf sparkConf;
    protected  JavaSparkContext context;
    protected String name;
    protected String model;
    private void setUpContext(){
        SparkConf sparkConf = new SparkConf().setAppName(name).setMaster(model);
        context=new JavaSparkContext(sparkConf);
    }
    public  void setNameAndModel(String name,String model){
        this.name=name;
        this.model=model;
    }
    public abstract void process();
    public void run(){
        setUpContext();
        process();
        context.close();
    }
}
