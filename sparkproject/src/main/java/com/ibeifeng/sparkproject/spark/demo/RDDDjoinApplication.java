package com.ibeifeng.sparkproject.spark.demo;

import com.ibeifeng.sparkproject.util.Constants;
import com.ibeifeng.sparkproject.util.SparkModelConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RDDDjoinApplication {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.USERVISI_TSESSIONANALYZER_SPARK)
                .setMaster(SparkModelConstants.LOCAL);

        JavaSparkContext ssc=new JavaSparkContext(conf);

        List<String> ss=new ArrayList<>();
        
        ss.add("111");
        ss.add("222");
        ss.add("222");
        ss.add("222");
        ss.add("222");
        ss.add("222");
        ss.add("222");
        ss.add("333");
        ss.add("444");
        ss.add("555");
        ss.add("666");
        ss.add("777");
        ss.add("888");
        ss.add("999");
        ss.add("10000");

        JavaRDD<String> parallelize = ssc.parallelize(ss);

        JavaRDD<String> filterRDD = parallelize.filter(new Function<String, Boolean>() {
            private int i=0;
            @Override
            public Boolean call(String v1) throws Exception {
                 i++;
                System.out.println(i);

                if (Long.valueOf(v1) > 111) {
                    return true;
                }

                return false;
            }
        });

        JavaPairRDD<Long, Long> longLongJavaPairRDD = filterRDD.mapToPair(new PairFunction<String, Long, Long>() {

            @Override
            public Tuple2<Long, Long> call(String s) throws Exception {


                Random ra =new Random();
                return new Tuple2<Long, Long>(Long.valueOf(s), Long.valueOf(s));
            }
        });

        longLongJavaPairRDD.count();

       longLongJavaPairRDD = longLongJavaPairRDD.distinct();
        longLongJavaPairRDD.count();

        ssc.close();
    }
}
