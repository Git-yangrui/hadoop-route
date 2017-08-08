package com.yangrui.hadoop

import org.apache.spark.{SparkConf, SparkContext}

object SparkApp {
  def main(args: Array[String]): Unit = {
//    var sparkConf=new

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val valueRDD=sc.textFile("").flatMap(_.split("")).map((_,1))

  }
}
