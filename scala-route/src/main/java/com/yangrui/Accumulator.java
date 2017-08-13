package com.yangrui;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Accumulator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("accumulator")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

    }
}
