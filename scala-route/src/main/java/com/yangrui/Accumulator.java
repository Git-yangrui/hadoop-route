package com.yangrui;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Accumulator {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("accumulator")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        final org.apache.spark.Accumulator<Integer> sum= context.accumulator(0);
        List<Integer> numlist= Arrays.asList(1,2,3,4,5,12,232,323,32,32323,323,2323,232,3232,323);

        JavaRDD<Integer> numbers = context.parallelize(numlist);
        numbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });
        System.out.println(sum);
        context.close();
    }
}
