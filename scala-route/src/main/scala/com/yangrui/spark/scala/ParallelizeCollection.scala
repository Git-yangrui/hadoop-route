package com.yangrui.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("scala").setMaster("local")
    val context = new SparkContext(conf)
    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val parallelizeCollection = context.parallelize(array,4)
    val result = parallelizeCollection.reduce(_ + _);
    println(result)
  }

}
