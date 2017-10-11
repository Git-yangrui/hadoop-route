package com.yangrui.hadoop

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeU {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("wordCount")
    conf.set("spark.testing.memory", "471859200")
    val ssc = new StreamingContext(conf, Seconds(3))

    val flumeStream = FlumeUtils.createStream(ssc,"",9999)

    val flumeMapDStream=flumeStream.flatMap(event =>
      event.event.getBody.array().toString.split(" ")

    )


  }

}
