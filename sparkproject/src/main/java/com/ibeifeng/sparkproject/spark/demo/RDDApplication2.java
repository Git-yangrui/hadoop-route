package com.ibeifeng.sparkproject.spark.demo;

import com.ibeifeng.sparkproject.util.Constants;
import com.ibeifeng.sparkproject.util.SparkModelConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RDDApplication2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.USERVISI_TSESSIONANALYZER_SPARK)
                .setMaster(SparkModelConstants.LOCAL);

        conf.set("spark.testing.memory", "471859200");

        JavaSparkContext ssc=new JavaSparkContext(conf);

        List<String> ss=new ArrayList<>();
        
        ss.add("111");
        ss.add("222");
        ss.add("333");
        ss.add("444");
        ss.add("555");
        ss.add("666");
        ss.add("777");
        ss.add("888");
        ss.add("999");
        ss.add("10000");

        JavaRDD<String> parallelize = ssc.parallelize(ss).repartition(3);


        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = parallelize.mapToPair(value -> new Tuple2<String, Integer>(value, 1));


        JavaPairRDD<String, Iterable<Integer>> stringIterableJavaPairRDD = stringIntegerJavaPairRDD.groupByKey();
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 = stringIterableJavaPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {

            @Override
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {


                Iterable<Integer> integers = stringIterableTuple2._2;

                List<Tuple2<String, Integer>> list = new ArrayList<>();
                Iterator<Integer> iterator = integers.iterator();
                int a = 0;
                while (iterator.hasNext()) {

                    a = a + iterator.next();
                }


                list.add(new Tuple2<String, Integer>(stringIterableTuple2._1, a));

                return list.iterator();
            }
        });
        stringIntegerJavaPairRDD1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

            }
        });

        ssc.close();
    }
}
