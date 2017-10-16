package com.yangrui.mrdesinmode;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CombineDemo {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");

        sparkConf.set("spark.testing.memory", "471859200");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        ArrayList<ScoreDetail> scoreDetails = new ArrayList<>();
        scoreDetails.add(new ScoreDetail("xiaoming", "Math", 98));
        scoreDetails.add(new ScoreDetail("xiaoming", "English", 88));
        scoreDetails.add(new ScoreDetail("wangwu", "Math", 75));
        scoreDetails.add(new ScoreDetail("wangwu", "Englist", 78));
        scoreDetails.add(new ScoreDetail("lihua", "Math", 90));
        scoreDetails.add(new ScoreDetail("lihua", "English", 80));
        scoreDetails.add(new ScoreDetail("zhangsan", "Math", 91));
        scoreDetails.add(new ScoreDetail("zhangsan", "English", 80));


        JavaRDD<ScoreDetail> parallelize = sc.parallelize(scoreDetails);

        JavaPairRDD<String, ScoreDetail> stringScoreDetailJavaPairRDD = parallelize.mapToPair(scoreDetail -> {


            return new Tuple2<String, ScoreDetail>(scoreDetail.studentName, scoreDetail);

        });


        JavaPairRDD<String, Tuple2<Float, Integer>> stringTuple2JavaPairRDD =
                stringScoreDetailJavaPairRDD.combineByKey(v1 -> {
                    return new Tuple2<Float, Integer>(v1.score, 1);
                }, (v1, v2) -> {
                    return new Tuple2<Float, Integer>(v1._1 + v2.score, v1._2 + 1);
                }, (v1, v2) -> {
                    return new Tuple2<Float, Integer>(v1._1 + v2._1, v1._2 + v2._2);
                }
        );
        JavaPairRDD<String, String> stringStringJavaPairRDD = stringTuple2JavaPairRDD.flatMapToPair(v1 -> {


            List<Tuple2<String, String>> a = new ArrayList<>();
            String stuNAme = v1._1;
            Float aFloat = v1._2._1;
            Integer integer = v1._2._2;
            float v = aFloat / integer;
            String s = String.valueOf(v);

            a.add(new Tuple2<String, String>(stuNAme, stuNAme + "\t" + s));

            return a.iterator();
        });


        stringStringJavaPairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {

            }
        });


        stringStringJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {

            }
        });
        stringStringJavaPairRDD.saveAsHadoopFile("/tmp/lxw12345/", NullWritable.class,Tuple2.class,RDDMultipleTextOutputFormat.class);
//, Text.class,Tuple2.class,RDDMultipleTextOutputFormat.class
//        stringTuple2JavaPairRDD.saveAsTextFile("/tmp/lxw1234/");

//        stringTuple2JavaPairRDD.saveAsHadoopFile();
    }



   static class  RDDMultipleTextOutputFormat<K,V> extends MultipleTextOutputFormat<K,V>{
        @Override
        protected String generateFileNameForKeyValue(K key, V value, String name) {
            return key+"/"+name;
        }
    }
}
