package com.yangrui.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOperation {

    private static JavaSparkContext context;

    public static void main(String[] args) {
        init();
        // mapOperation();
        joinOperation();
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

    /**
     * map function demo
     */
    private static void mapOperation() {
        JavaRDD<Integer> parallelize = context.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaRDD<Integer> mapRDD = parallelize.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 10;
            }
        });
        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("--------------" + integer);
            }
        });
    }

    /**
     * filter function demo
     */
    private static void filterOperation() {
        JavaRDD<Integer> parallelize = context.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        JavaRDD<Integer> filter = parallelize.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                if (v1 % 2 == 0) {
                    return true;
                }
                return false;
            }
        });

        filter.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * groupByKey
     */
    private static void groupbyKeyOperation() {
        List<Tuple2<String, Integer>> scores = new ArrayList<>();
        scores.add(new Tuple2<String, Integer>("class1", 90));
        scores.add(new Tuple2<String, Integer>("class2", 70));
        scores.add(new Tuple2<String, Integer>("class1", 60));
        scores.add(new Tuple2<String, Integer>("class2", 80));
        scores.add(new Tuple2<String, Integer>("class1", 70));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = context.parallelizePairs(scores);
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        stringIterableJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1 + "         " + stringIterableTuple2._2);
            }
        });
    }

    /**
     * groupByKey 2 demo
     */
    private static void groupbyKeyOperation1() {
        List<Tuple2<String, Integer>> scores = new ArrayList<>();
        scores.add(new Tuple2<String, Integer>("class1", 90));
        scores.add(new Tuple2<String, Integer>("class2", 70));
        scores.add(new Tuple2<String, Integer>("class1", 60));
        scores.add(new Tuple2<String, Integer>("class2", 80));
        scores.add(new Tuple2<String, Integer>("class1", 70));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = context.parallelizePairs(scores);
        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        stringIterableJavaPairRDD.map(new Function<Tuple2<String,Iterable<Integer>>, Object>() {
            @Override
            public Object call(Tuple2<String, Iterable<Integer>> v1) throws Exception {
                Iterable<Integer> integers = v1._2;
                Iterator<Integer> iterator = integers.iterator();
                return null;
            }
        });
    }

    private static void sortbyKeyOperation1() {
        List<Tuple2<Integer, String>> scores = new ArrayList<>();
        scores.add(new Tuple2<Integer, String>(65,"leo"));
        scores.add(new Tuple2<Integer, String>(56,"tom"));
        scores.add(new Tuple2<Integer, String>(100,"marry"));
        scores.add(new Tuple2<Integer, String>(80,"jack"));

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = context.parallelizePairs(scores);
        JavaPairRDD<Integer, String> integerStringJavaPairRDD1 = integerStringJavaPairRDD.sortByKey(false);
        integerStringJavaPairRDD1.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._1+"   "+integerStringTuple2._2);
            }
        });

    }
    private static void joinOperation() {
        List<Tuple2<Integer, String>> studentList = new ArrayList<>();
        studentList.add(new Tuple2<Integer, String>(1,"leo"));
        studentList.add(new Tuple2<Integer, String>(2,"tom"));
        studentList.add(new Tuple2<Integer, String>(3,"marry"));

        List<Tuple2<Integer, Integer>> scores = new ArrayList<>();
        scores.add(new Tuple2<Integer, Integer>(1,90));
        scores.add(new Tuple2<Integer, Integer>(2,80));
        scores.add(new Tuple2<Integer, Integer>(3,100));
        JavaPairRDD<Integer, String> integerStringJavaPairRDD = context.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = context.parallelizePairs(scores);
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinResults = integerStringJavaPairRDD.join(integerIntegerJavaPairRDD);
        joinResults.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2) throws Exception {
                System.out.print(integerTuple2Tuple2._1+"  ");
                Tuple2<String, Integer> stringIntegerTuple2 = integerTuple2Tuple2._2;
                System.out.print(stringIntegerTuple2._1+"  ");
                System.out.print(stringIntegerTuple2._2);
                System.out.println();
                System.out.println("========================");
            }
        });
    }
}
