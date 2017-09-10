package com.ibeifeng.sparkproject.spark;

import com.ibeifeng.sparkproject.util.Constants;
import com.ibeifeng.sparkproject.util.SparkModelConstants;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ParallelizePairs {

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


        JavaPairRDD<String, String> stringStringJavaPairRDD = ssc.parallelizePairs(ss);
        stringStringJavaPairRDD.take(10);

        ssc.close();
    }
}
