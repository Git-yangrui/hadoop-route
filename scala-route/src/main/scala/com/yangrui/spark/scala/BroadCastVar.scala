package com.yangrui.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

object BroadCastVar {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("BroadCatVar").setMaster("local")
    val context=new SparkContext(conf)
    var factor=3
    val factorBroadCasr=context.broadcast(factor)
    val numberLIst=Array(1,2,3,4,5)
    val list=context.parallelize(numberLIst)
    val test=list.map( num => num * factorBroadCasr.value)
    test.foreach(num => println(num))
    context.stop()
  }
}
