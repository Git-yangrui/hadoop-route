package com.yangrui.scla

import org.apache.spark.{SparkConf, SparkContext}

object SparkApp {
  def main(args: Array[String]): Unit = {
    val logFile = "hdfs://192.168.1.100:9000/hadoop.txt"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile(logFile)

    val wordcount = rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    val test=wordcount.collect()
    wordcount.saveAsTextFile("hdfs://192.168.1.100:9000/user/output2")

    sc.stop()
  }
}
