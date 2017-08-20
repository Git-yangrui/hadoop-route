package com.yangrui.spark;

import com.sun.org.apache.xerces.internal.dom.PSVIAttrNSImpl;
import com.yangrui.MySparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//need  implement serializable
public class WorldCountSort extends MySparkContext implements  Serializable{

    @Override
    public void process() {
        JavaRDD<String> stringJavaRDD = context.textFile("C:\\Users\\admin\\Desktop\\112.txt");
        JavaRDD<String> linesRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pariRDD = linesRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> reduceResultRDD = pariRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        JavaPairRDD<Integer, String> reverseRDD = reduceResultRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });
        JavaPairRDD<Integer, String> sortRDD = reverseRDD.sortByKey(false);
        JavaPairRDD<String, Integer> reversAgainRDD = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<String, Integer>(integerStringTuple2._2, integerStringTuple2._1);
            }
        });
        reversAgainRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"  "+stringIntegerTuple2._2);
            }
        });
    }

    public static void main(String[] args) {
        WorldCountSort worldCountSort=new WorldCountSort();
        worldCountSort.setNameAndModel("WorldCountSort","local");
        worldCountSort.run();
    }
}
