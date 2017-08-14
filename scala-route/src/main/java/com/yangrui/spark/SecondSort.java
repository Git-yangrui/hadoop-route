package com.yangrui.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class SecondSort {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SecondSort").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lineRDD = javaSparkContext.textFile("C:\\Users\\admin\\Desktop\\123.txt");
        JavaPairRDD<SecondSortKey, String> secondSortKeyStringJavaPairRDD = lineRDD.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String s) throws Exception {
                String[] splits = s.split(" ");
                SecondSortKey ket = new SecondSortKey(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]));
                return new Tuple2<SecondSortKey, String>(ket, s);
            }
        });
        JavaPairRDD<SecondSortKey, String> secondSortKeyStringJavaPairRDD1 = secondSortKeyStringJavaPairRDD.sortByKey(false);
        JavaRDD<String> map = secondSortKeyStringJavaPairRDD1.map(new Function<Tuple2<SecondSortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        map.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        javaSparkContext.close();
    }
}
