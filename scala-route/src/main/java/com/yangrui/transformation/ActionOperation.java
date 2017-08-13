package com.yangrui.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ActionOperation {
    private static JavaSparkContext context;

    public static void main(String[] args) {
        init();

        close();
    }

    private static void close() {
        context.close();
    }

    private static void init() {
        SparkConf conf = new SparkConf().setAppName("operation")
                .setMaster("local");
        context = new JavaSparkContext(conf);
    }


}
