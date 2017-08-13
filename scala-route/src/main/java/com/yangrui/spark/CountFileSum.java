package com.yangrui.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * count file size
 */
public class CountFileSum {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParellelizeCollection")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = context.textFile("C:\\Users\\admin\\Desktop\\112.txt");
        JavaRDD<Integer> integerJavaRDD = stringJavaRDD.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                System.out.println(v1);
                return v1.length();
            }
        });

        Integer reduce = integerJavaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(reduce);
        context.close();
    }

}
