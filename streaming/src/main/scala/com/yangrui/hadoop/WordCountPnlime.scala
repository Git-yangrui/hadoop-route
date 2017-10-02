package com.yangrui.hadoop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCountPnlime {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("wordCount")
    val ssc = new StreamingContext(conf, Seconds(1))


    val dStream = ssc.socketTextStream("192.168.1.100", 9999)
    val woreds = dStream.flatMap(_.split(" "))
    val pairs = woreds.map(word => (word, 1))

    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()

    ssc.awaitTermination()

  }

}
