package com.yangrui.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountCluster {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WorldCountCluster");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("hdfs://yangziyu01:9000/user/spark/112.txt");
        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t"));
            }
        });
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = stringJavaRDD1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIntegerJavaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        long count = stringIntegerJavaPairRDD1.count();
        stringIntegerJavaPairRDD1.saveAsTextFile("hdfs://yangziyu01:9000/user/spark/out/");

        javaSparkContext.close();
    }
}
