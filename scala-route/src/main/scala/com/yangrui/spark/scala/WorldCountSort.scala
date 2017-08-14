package com.yangrui.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object WorldCountSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WorldCountSort").setMaster("local")
    val context = new SparkContext(conf)
    val lineRDD = context.textFile("C:\\Users\\admin\\Desktop\\112.txt")
    val linesRDD = lineRDD.flatMap(line => line.split(" ")).map(word => (word, 1))
      .reduceByKey((_ + _)).map(tt => (tt._2, tt._1)).sortByKey(false).map(tuple => (tuple._2, tuple._1)).foreach(line => println(line))
    context.stop()
  }

}
