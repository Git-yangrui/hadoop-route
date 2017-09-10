package com.ibeifeng.sparkproject.spark;

import com.ibeifeng.sparkproject.util.Constants;
import com.ibeifeng.sparkproject.util.SparkModelConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JOinPairs {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.USERVISI_TSESSIONANALYZER_SPARK)
                .setMaster(SparkModelConstants.LOCAL);
        conf.set("spark.testing.memory", "471859200");
        JavaSparkContext ssc=new JavaSparkContext(conf);

        List<Tuple2<String,String>> ss=new ArrayList<>();
        
        ss.add(new Tuple2<String,String>("111","111"));
        ss.add(new Tuple2<String,String>("222","111"));
        ss.add(new Tuple2<String,String>("333","111"));
        ss.add(new Tuple2<String,String>("444","111"));
        ss.add(new Tuple2<String,String>("555","111"));
        ss.add(new Tuple2<String,String>("666","111"));
        ss.add(new Tuple2<String,String>("777","111"));
        ss.add(new Tuple2<String,String>("888","111"));
        ss.add(new Tuple2<String,String>("999","111"));
        List<Tuple2<String,String>> ss1=new ArrayList<>();

        ss1.add(new Tuple2<String,String>("111","1110"));
        ss1.add(new Tuple2<String,String>("222","1110"));
        ss1.add(new Tuple2<String,String>("333","1110"));

        JavaPairRDD<String, String> stringStringJavaPairRDD = ssc.parallelizePairs(ss);

        JavaPairRDD<String, String> stringStringJavaPairRDD1 =
                ssc.parallelizePairs(ss1);

        JavaPairRDD<String, Tuple2<String, String>> join =
                stringStringJavaPairRDD1.join(stringStringJavaPairRDD);


        JavaRDD<String> map = stringStringJavaPairRDD1.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1;
            }
        });

        map.take(10);

        ssc.close();
    }
}
