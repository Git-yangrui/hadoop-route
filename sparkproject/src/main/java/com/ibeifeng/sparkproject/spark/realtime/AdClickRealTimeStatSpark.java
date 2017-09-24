package com.ibeifeng.sparkproject.spark.realtime;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class AdClickRealTimeStatSpark {

    public static void main(String[] args)  throws  Exception{
        SparkConf conf=new SparkConf().setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");


        JavaStreamingContext streamingContext=new JavaStreamingContext(conf, Durations.seconds(5));
        //
        Map<String,String> kafakaParamMap=new HashMap<>();

        kafakaParamMap.put("metadata.broker.list","192.168.1.100:9092,192.168.1.101:9092,192.168.1.102:9092");

        Set<String> kafakaTopics=new HashSet<>();
        kafakaTopics.add("AdRealTimeTopic");
        JavaPairInputDStream<String,String> adRealtimeDstream= KafkaUtils.createDirectStream(
                streamingContext,String.class, String.class,
                        StringDecoder.class,StringDecoder.class,kafakaParamMap,kafakaTopics);

        
        //首先过滤掉那些黑名单人

//        JavaPairDStream<String, String> filterBlackListRdd = filterByBlacklist(adRealtimeDstream);
//        JavaPairDStream<String, String> stringStringJavaPairDStream = adRealtimeDstream.mapToPair(stringStringTuple2 -> {
//
//            return stringStringTuple2;
//
//        });

//        generateDynamicBlacklist(filterBlackListRdd);

        streamingContext.start();

        streamingContext.awaitTermination();

        streamingContext.close();
    }

    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filterBlackListRdd) {
        //timestamp  provice city userid adid
        JavaPairDStream<String, Long> mapToRDD = filterBlackListRdd.mapToPair(stringStringTuple2 -> {
            String log = stringStringTuple2._2;
            String[] splits = log.split(" ");
            String timestamp = splits[0];
            Date date = new Date(Long.valueOf(timestamp));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            String formatDate = simpleDateFormat.format(date);
//            String provice=splits[1];
//            String city=splits[2];
            String userid = splits[3];
            String adid = splits[4];
            String key = formatDate + "_" + userid + "_" + adid;
            return new Tuple2<String, Long>(key, 1L);

        });

        JavaPairDStream<String, Long> eachUserAdidCount = mapToRDD.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
//        eachUserAdidCount.reduceByKeyAndWindow();
        eachUserAdidCount.foreachRDD(eachJavaPairRDD -> {

            eachJavaPairRDD.foreachPartition(tuple2Iterator -> {

                List<Object> list=new ArrayList<>();

                if(tuple2Iterator.hasNext()){
                    Tuple2<String, Long> next = tuple2Iterator.next();
                    String info = next._1;
                    Long count = next._2;
                    //封装在一个domain中然后存在数据库中
                    //可以存在hbase中
                    list.add(new Object());
                }

//                persist(list) 将数据存到数据库中，调用hbase api...................................
            });
        });


        JavaPairDStream<String, Long> filter = eachUserAdidCount.filter(v1 -> {
            String s = v1._1;

            //去数据库查询当前user的点击数量，如果 数据库中count >=100 则return ture
            int count = 0;
            if (count >= 100) {
                return true;
            }
            return false;
        });

        //user去重
        JavaDStream<Long> mapUserid = filter.map(v1 -> {
            String userid = v1._1.split("\\_")[1];
            return Long.valueOf(userid);
        });

        JavaDStream<Long> transformDistinctRDD = mapUserid.transform(v1 -> {
            return v1.distinct();
        });



        //需要来和的一些用户
        transformDistinctRDD.foreachRDD(stringLongJavaPairRDD -> {
            stringLongJavaPairRDD.foreachPartition(tuple2Iterator -> {

                //批量的将一些用户拉黑
            });

        });



    }

//
//    private static JavaPairDStream<String, String> filterByBlacklist(
//            JavaPairInputDStream<String, String> adRealTimeLogDStream) {
//        // 刚刚接受到原始的用户点击行为日志之后
//        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
//        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
//
////        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
////
////                new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {
////
////                    private static final long serialVersionUID = 1L;
////
////                    @SuppressWarnings("resource")
////                    @Override
////                    public JavaPairRDD<String, String> call(
////                            JavaPairRDD<String, String> rdd) throws Exception {
////
////                        // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
////                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
////                        List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();
////
////                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
////
////                        for(AdBlacklist adBlacklist : adBlacklists) {
////                            tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
////                        }
////
////                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
////                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);
////
////                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
////                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {
////
////                            private static final long serialVersionUID = 1L;
////
////                            @Override
////                            public Tuple2<Long, Tuple2<String, String>> call(
////                                    Tuple2<String, String> tuple)
////                                    throws Exception {
////                                String log = tuple._2;
////                                String[] logSplited = log.split(" ");
////                                long userid = Long.valueOf(logSplited[3]);
////                                return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
////                            }
////
////                        });
////
////                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
////                        // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
////                        // 用inner join，内连接，会导致数据丢失
////
////                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD =
////                                mappedRDD.leftOuterJoin(blacklistRDD);
////
////                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
////
////                                new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {
////
////                                    private static final long serialVersionUID = 1L;
////
////                                    @Override
////                                    public Boolean call(
////                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
////                                            throws Exception {
////                                        Optional<Boolean> optional = tuple._2._2;
////
////                                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
////                                        if(optional.isPresent() && optional.get()) {
////                                            return false;
////                                        }
////
////                                        return true;
////                                    }
////
////                                });
////
////                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
////
////                                new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String, String>() {
////
////                                    private static final long serialVersionUID = 1L;
////
////                                    @Override
////                                    public Tuple2<String, String> call(
////                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
////                                            throws Exception {
////                                        return tuple._2._1;
////                                    }
////
////                                });
////
////                        return resultRDD;
////                    }
////
////                });
////
////        return filteredAdRealTimeLogDStream;
//    }
}
