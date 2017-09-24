package com.ibeifeng.sparkproject.spark.streamingha;

import org.apache.spark.streaming.receiver.Receiver;

public class StreamingHa {


    public static void main(String[] args) {
//        SparkStreaming.checkpoint("hdfs://192.168.1.105:9090/checkpoint")
//
//        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
//            @Override
//            public JavaStreamingContext create() {
//                JavaStreamingContext jssc = new JavaStreamingContext(...);
//                JavaDStream<String> lines = jssc.socketTextStream(...);
//                jssc.checkpoint(checkpointDirectory);
//                return jssc;
//            }
//        };
//
//
//
// JavaStreamingContext context =
//                      JavaStreamingContext.getOrCreate(checkpointDirectory, contextFactory);
//        context.start();
//        context.awaitTermination();


//        spark-submit
//                --deploy-mode cluster
//                --supervise
//

//        Receiver 主要是接受数据，，  那么立即接受到数据之后会将数据写到容错系统比如hdfs 上


//        WAL  (Write Ahead LOg)


//        spark.streaming.receiver.writeAheadLog.enable

    }
}
