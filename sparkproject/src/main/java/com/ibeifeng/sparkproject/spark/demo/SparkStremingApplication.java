package com.ibeifeng.sparkproject.spark.demo;

import com.ibeifeng.sparkproject.util.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.Iterator;

public class SparkStremingApplication {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName(Constants.SPARKSTREMINGAPPLICATION).setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.1.100", 9999, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream1 = stringIntegerJavaPairDStream.
                reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                });
        stringIntegerJavaPairDStream1.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
