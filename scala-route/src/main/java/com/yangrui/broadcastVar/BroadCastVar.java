package com.yangrui.broadcastVar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.List;
//创建共享变量
public class BroadCastVar {
    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("broadcatVariable")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        final int factor =3;
       final org.apache.spark.broadcast.Broadcast<Integer> broadcastFactor = context.broadcast(factor);
        List<Integer> numberList= java.util.Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = context.parallelize(numberList);
        JavaRDD<Integer> afterFactoMap = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * broadcastFactor.getValue();
            }
        });
        afterFactoMap.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        context.close();
    }
}
