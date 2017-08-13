package com.yangrui.RDDPersist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDPersist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Persist")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> linesRDD = context.textFile("C:\\Users\\admin\\Desktop\\112.txt").cache();
        long startTimes=System.currentTimeMillis();
        long count = linesRDD.count();
        long endTimes=System.currentTimeMillis();
        System.out.println(endTimes-startTimes);

        System.out.println("=========================="+count);

        long startTimes1=System.currentTimeMillis();
        long count1 = linesRDD.count();
        long endTimes1=System.currentTimeMillis();
        System.out.println(endTimes1-startTimes1);
        System.out.println("=========================="+count1);

        long startTimes2=System.currentTimeMillis();
        long count2 = linesRDD.count();
        long endTimes2=System.currentTimeMillis();
        System.out.println(endTimes2-startTimes2);
        System.out.println("=========================="+count2);
    }
}
