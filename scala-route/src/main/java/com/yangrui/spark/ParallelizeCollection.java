package com.yangrui.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * sum( 1 to 10 )
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParellelizeCollection")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
//        SparkContext context=new SparkContext(conf);
//        context.
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> parallelize = context.parallelize(numbers);
//        context.parallelize()
        Integer reduce = parallelize.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(reduce);
        context.close();
    }


}
