package com.yangrui.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object Accumulatord {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("BroadCatVar").setMaster("local")
    val context=new SparkContext(conf)
    val accumulatord=context.accumulator(0)
    val numList=Array(1,2,3,4,5,6,6)
    val numbers = context.parallelize(numList,1)
    numbers.foreach( num => {accumulatord += num})

    println(accumulatord)
    context.stop()
  }
}
